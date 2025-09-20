// OpenRouter-specific types and structures
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OpenRouter routing strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RouteStrategy {
    /// Automatic fallback to alternative models
    Fallback,
}

impl Default for RouteStrategy {
    fn default() -> Self {
        RouteStrategy::Fallback
    }
}

/// Provider preferences for OpenRouter
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderPreferences {
    /// Allow specific providers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow: Option<Vec<String>>,

    /// Disallow specific providers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disallow: Option<Vec<String>>,

    /// Require specific provider features
    #[serde(skip_serializing_if = "Option::is_none")]
    pub require: Option<Vec<String>>,
}

impl ProviderPreferences {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allow(mut self, providers: Vec<String>) -> Self {
        self.allow = Some(providers);
        self
    }

    pub fn disallow(mut self, providers: Vec<String>) -> Self {
        self.disallow = Some(providers);
        self
    }

    pub fn require(mut self, features: Vec<String>) -> Self {
        self.require = Some(features);
        self
    }
}

/// Transform types for prompt preprocessing
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transform {
    /// Middle-out transform for longer context
    MiddleOut,
}

/// OpenRouter model information with pricing and features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenRouterModel {
    /// Model identifier
    pub id: String,

    /// Human-readable name
    pub name: String,

    /// Model description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Context window size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_length: Option<u32>,

    /// Pricing information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pricing: Option<ModelPricing>,

    /// Top provider for this model
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_provider: Option<ProviderInfo>,

    /// Per-message token limit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_request_limits: Option<RequestLimits>,
}

/// Model pricing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPricing {
    /// Cost per 1M prompt tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,

    /// Cost per 1M completion tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion: Option<String>,

    /// Cost per 1M input tokens (for unified pricing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,

    /// Cost per 1M output tokens (for unified pricing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

/// Provider information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInfo {
    /// Provider name
    pub name: String,

    /// Maximum context length for this provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,

    /// Whether provider supports streaming
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_moderated: Option<bool>,
}

/// Per-request limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestLimits {
    /// Maximum prompt tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u32>,

    /// Maximum completion tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u32>,
}

/// Generation tracking information from OpenRouter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationInfo {
    /// Unique generation identifier
    pub id: String,

    /// Model used for generation
    pub model: String,

    /// Provider that served the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,

    /// Total cost in USD
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost: Option<f64>,

    /// Token usage information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<GenerationUsage>,

    /// Request timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,

    /// Additional metadata
    #[serde(flatten)]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Detailed token usage for generation tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationUsage {
    /// Prompt tokens used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u32>,

    /// Completion tokens generated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u32>,

    /// Total tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u32>,

    /// Native provider token counts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_tokens_prompt: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_tokens_completion: Option<u32>,
}

/// OpenRouter rate limits and quotas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenRouterLimits {
    /// Daily credit limit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_limit: Option<f64>,

    /// Credits used today
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_used: Option<f64>,

    /// Credits remaining today
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_remaining: Option<f64>,

    /// Rate limit per minute
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<u32>,

    /// Requests remaining in current window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_remaining: Option<u32>,

    /// Reset time for rate limit window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_reset: Option<String>,
}

/// OpenRouter-specific chat completion request
#[derive(Debug, Clone, Serialize)]
pub struct OpenRouterChatRequest {
    /// Model to use (or primary model if using multiple)
    pub model: String,

    /// Array of models for fallback routing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models: Option<Vec<String>>,

    /// Messages for the conversation
    pub messages: Vec<crate::llm::models::ChatMessage>,

    /// Maximum tokens to generate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    /// Temperature for randomness
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// Top-p sampling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    /// Top-k sampling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,

    /// Frequency penalty
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,

    /// Presence penalty
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,

    /// Repetition penalty (alternative to frequency)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repetition_penalty: Option<f32>,

    /// Stop sequences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,

    /// Enable streaming
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    /// Routing strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<RouteStrategy>,

    /// Provider preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderPreferences>,

    /// Transform types
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<Transform>>,

    /// Response format for structured output
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat>,

    /// Tools available to the model
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<crate::llm::models::Tool>>,

    /// Tool choice strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<crate::llm::models::ToolChoice>,

    /// User identifier for abuse prevention
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Response format for structured output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFormat {
    /// Format type (e.g., "json_object")
    #[serde(rename = "type")]
    pub format_type: String,

    /// JSON schema for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
}

/// OpenRouter-specific chat completion response
#[derive(Debug, Clone, Deserialize)]
pub struct OpenRouterChatResponse {
    /// Response identifier
    pub id: String,

    /// Object type (always "chat.completion")
    pub object: String,

    /// Creation timestamp
    pub created: u64,

    /// Model used for generation
    pub model: String,

    /// Choice options
    pub choices: Vec<OpenRouterChoice>,

    /// Token usage information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<crate::llm::models::Usage>,

    /// Provider that served the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
}

/// OpenRouter choice in response
#[derive(Debug, Clone, Deserialize)]
pub struct OpenRouterChoice {
    /// Choice index
    pub index: u32,

    /// Generated message
    pub message: crate::llm::models::ChatMessage,

    /// Finish reason
    pub finish_reason: Option<String>,

    /// Log probabilities (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<serde_json::Value>,
}

/// OpenRouter streaming response chunk
#[derive(Debug, Clone, Deserialize)]
pub struct OpenRouterStreamChunk {
    /// Chunk identifier
    pub id: String,

    /// Object type
    pub object: String,

    /// Creation timestamp
    pub created: u64,

    /// Model being used
    pub model: String,

    /// Choice deltas
    pub choices: Vec<OpenRouterStreamChoice>,
}

/// OpenRouter streaming choice delta
#[derive(Debug, Clone, Deserialize)]
pub struct OpenRouterStreamChoice {
    /// Choice index
    pub index: u32,

    /// Delta information
    pub delta: OpenRouterStreamDelta,

    /// Finish reason (if this is the last chunk)
    pub finish_reason: Option<String>,
}

/// OpenRouter streaming delta content
#[derive(Debug, Clone, Deserialize)]
pub struct OpenRouterStreamDelta {
    /// Role delta (usually only in first chunk)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Content delta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,

    /// Tool calls delta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<crate::llm::models::ToolCall>>,
}