use std::env;
use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use opentelemetry::trace::Span;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Result as SdkResult, SdkError};

use super::http;
use super::interface::{
    generate as generate_via_model, stream as stream_via_model, BuiltInTool, ContentBlockType,
    GenerateRequest, GenerateResponse, LanguageModel, Modality, ReasoningEffort, ResponseFormat,
    StreamChunk, StreamHandle, StreamRequest, TokenUsage, ToolCall,
};
use super::telemetry;

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_RESPONSES_PATH: &str = "responses";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600); // 10 minutes to match official OpenAI SDK
const DEFAULT_MODEL_PREFIX: &str = "openai";

/// Configuration for OpenAI Responses API.
/// This is the default and recommended provider for OpenAI's official API.
/// For third-party OpenAI-compatible APIs, use `OpenAiChatProvider`.
#[derive(Clone, Debug)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub base_url: String,
    pub responses_path: String,
    pub organization: Option<String>,
    pub project: Option<String>,
    pub timeout: Duration,
    pub extra_headers: Vec<(String, String)>,
    pub model_prefix: Option<String>,
    pub retry_config: http::RetryConfig,
}

impl OpenAiConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            responses_path: DEFAULT_RESPONSES_PATH.to_string(),
            organization: None,
            project: None,
            timeout: DEFAULT_TIMEOUT,
            extra_headers: Vec::new(),
            model_prefix: Some(DEFAULT_MODEL_PREFIX.to_string()),
            retry_config: http::RetryConfig::from_env(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_responses_path(mut self, path: impl Into<String>) -> Self {
        self.responses_path = path.into();
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_organization(mut self, organization: impl Into<String>) -> Self {
        self.organization = Some(organization.into());
        self
    }

    pub fn with_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.push((key.into(), value.into()));
        self
    }

    pub fn with_model_prefix(mut self, prefix: Option<impl Into<String>>) -> Self {
        self.model_prefix = prefix.map(Into::into);
        self
    }

    pub fn from_env() -> SdkResult<Self> {
        let api_key = env::var("OPENAI_API_KEY").map_err(|_| SdkError::Configuration {
            message: "OPENAI_API_KEY must be set".to_string(),
            field: Some("OPENAI_API_KEY".to_string()),
        })?;

        let mut config = OpenAiConfig::new(api_key);

        if let Ok(base_url) = env::var("OPENAI_BASE_URL") {
            if !base_url.trim().is_empty() {
                config.base_url = base_url;
            }
        }

        if let Ok(organization) = env::var("OPENAI_ORGANIZATION") {
            if !organization.trim().is_empty() {
                config.organization = Some(organization);
            }
        }

        if let Ok(project) = env::var("OPENAI_PROJECT") {
            if !project.trim().is_empty() {
                config.project = Some(project);
            }
        }

        if let Ok(timeout) = env::var("OPENAI_REQUEST_TIMEOUT_SECS") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.timeout = Duration::from_secs(secs);
            }
        }

        Ok(config)
    }
}

/// Provider implementation for OpenAI Responses API.
/// This is the default and recommended provider for OpenAI's official API.
/// Uses the `/v1/responses` endpoint with support for built-in tools,
/// reasoning controls, and event-driven streaming.
#[derive(Clone)]
pub struct OpenAiProvider {
    http: Client,
    config: OpenAiConfig,
}

impl OpenAiProvider {
    pub fn new(config: OpenAiConfig) -> SdkResult<Self> {
        let http = http::build_http_client(config.timeout)?;

        Ok(Self { http, config })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = OpenAiConfig::from_env()?;
        Self::new(config)
    }

    fn request(&self) -> reqwest::RequestBuilder {
        let base = self.config.base_url.trim_end_matches('/');
        let path = self.config.responses_path.trim_start_matches('/');
        let url = format!("{base}/{path}");

        let mut builder = self
            .http
            .post(url)
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .header("Content-Type", "application/json");

        if let Some(org) = &self.config.organization {
            builder = builder.header("OpenAI-Organization", org);
        }

        if let Some(project) = &self.config.project {
            builder = builder.header("OpenAI-Project", project);
        }

        for (key, value) in &self.config.extra_headers {
            builder = builder.header(key, value);
        }

        builder
    }

    fn normalize_model(&self, model: &str) -> SdkResult<String> {
        let trimmed = model.trim();
        if trimmed.is_empty() {
            return Err(SdkError::Configuration {
                message: "model id must not be empty for OpenAI Responses requests".to_string(),
                field: Some("model".to_string()),
            });
        }

        match &self.config.model_prefix {
            Some(prefix) => {
                if let Some((provider, rest)) = trimmed.split_once('/') {
                    let rest = rest.trim();
                    if provider != prefix {
                        return Err(SdkError::Configuration {
                            message: format!("expected model prefix `{prefix}/`, got `{provider}`"),
                            field: Some("model".to_string()),
                        });
                    }
                    if rest.is_empty() {
                        return Err(SdkError::Configuration {
                            message: format!("model id must follow `{prefix}/` prefix"),
                            field: Some("model".to_string()),
                        });
                    }
                    Ok(rest.to_string())
                } else {
                    Err(SdkError::Configuration {
                        message: format!("model should be prefixed with `{prefix}/`"),
                        field: Some("model".to_string()),
                    })
                }
            }
            None => Ok(trimmed.to_string()),
        }
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        generate_via_model(self, request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        stream_via_model(self, request).await
    }
}

// ============================================================================
// Responses API Request Types
// ============================================================================

/// Input for the Responses API - can be simple text or structured items
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum InputType {
    /// Simple string input
    Simple(String),
    /// Structured item array (messages, function calls, function outputs)
    Items(Vec<InputItem>),
}

/// Input item types for Responses API
/// Supports messages, function calls, and function call outputs
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type")]
pub enum InputItem {
    /// Regular message item
    #[serde(rename = "message")]
    Message(ApiMessage),
    /// Function call (from assistant's tool calls)
    #[serde(rename = "function_call")]
    FunctionCall {
        call_id: String,
        name: String,
        arguments: String,
    },
    /// Function call output (tool result)
    #[serde(rename = "function_call_output")]
    FunctionCallOutput { call_id: String, output: String },
}

/// Message format for Responses API
#[derive(Clone, Debug, Serialize)]
pub struct ApiMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// Convert SDK Message to InputItem(s)
/// A single message may produce multiple items (e.g., assistant + function calls)
fn message_to_input_items(msg: &super::interface::Message) -> Vec<InputItem> {
    let mut items = Vec::new();

    // Check if this is a tool result message
    if let Some(tool_call_id) = &msg.tool_call_id {
        items.push(InputItem::FunctionCallOutput {
            call_id: tool_call_id.clone(),
            output: msg.content.clone(),
        });
        return items;
    }

    // Check if this is an assistant message with tool calls
    if let Some(tool_calls) = &msg.tool_calls {
        // Add assistant message with content (if any)
        if !msg.content.is_empty() {
            items.push(InputItem::Message(ApiMessage {
                role: "assistant".to_string(),
                content: Some(msg.content.clone()),
            }));
        }

        // Add function_call items for each tool call
        for tc in tool_calls {
            items.push(InputItem::FunctionCall {
                call_id: tc.id.clone(),
                name: tc.name.clone(),
                arguments: tc.arguments.clone(),
            });
        }
        return items;
    }

    // Regular message
    let role = match msg.role {
        super::interface::MessageRole::User => "user".to_string(),
        super::interface::MessageRole::Assistant => "assistant".to_string(),
        super::interface::MessageRole::System => "system".to_string(),
    };

    items.push(InputItem::Message(ApiMessage {
        role,
        content: Some(msg.content.clone()),
    }));

    items
}

/// Reasoning configuration for o-series models
#[derive(Clone, Debug, Serialize)]
pub struct ReasoningConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>, // "minimal", "medium", "high"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<bool>,
}

/// API tool definition for Responses API (user-defined tools, not built-in)
/// Note: Responses API uses a flat structure, unlike Chat Completions API which nests under "function"
#[derive(Clone, Debug, Serialize)]
pub struct ApiTool {
    #[serde(rename = "type")]
    pub tool_type: String, // "function"
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parameters: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

/// Built-in tool for Responses API
#[derive(Clone, Debug, Serialize)]
pub struct ApiBuiltInTool {
    #[serde(rename = "type")]
    pub tool_type: String, // "web_search_preview", "code_interpreter", "file_search"
}

/// Complete Responses API request payload
#[derive(Clone, Debug, Serialize)]
pub struct ResponsesApiRequest {
    pub model: String,
    pub input: InputType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<bool>, // Default: false (stateless)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>, // Enable streaming responses
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Value>>, // Mix of ApiTool and ApiBuiltInTool
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasoningConfig>,
    /// Structured output format for Responses API (replaces response_format)
    /// Uses text.format instead of response_format for the Responses API
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Value>,
}

impl ResponsesApiRequest {
    pub fn from_request(req: &GenerateRequest, model: String) -> Self {
        // Build input
        let input = if req.messages.is_empty() {
            // Simple string input from system prompt
            InputType::Simple(req.system_prompt.clone().unwrap_or_default())
        } else {
            // Convert messages to input items (handles tool results)
            let items: Vec<InputItem> = req
                .messages
                .iter()
                .flat_map(message_to_input_items)
                .collect();
            InputType::Items(items)
        };

        // System prompt becomes instructions (separate from messages)
        let instructions = req.system_prompt.clone();

        // Build tools array (mix of user-defined and built-in)
        let mut tools_array: Vec<Value> = Vec::new();

        // Add user-defined function tools (flat structure for Responses API)
        for tool in &req.tools {
            let api_tool = ApiTool {
                tool_type: "function".to_string(),
                name: tool.name.clone(),
                description: tool.description.clone(),
                parameters: tool.parameters.clone().unwrap_or(serde_json::json!({})),
                strict: tool.strict,
            };
            tools_array.push(serde_json::to_value(api_tool).unwrap());
        }

        // Add built-in tools
        for built_in_tool in &req.config.built_in_tools {
            let tool_type = match built_in_tool {
                BuiltInTool::WebSearch => "web_search_preview",
                BuiltInTool::CodeInterpreter => "code_interpreter",
                BuiltInTool::FileSearch => "file_search",
                // OpenAI's web_search_preview already returns fetched content;
                // there's no separate web_fetch built-in to wire.
                BuiltInTool::WebFetch => continue,
            };
            let api_built_in_tool = ApiBuiltInTool {
                tool_type: tool_type.to_string(),
            };
            tools_array.push(serde_json::to_value(api_built_in_tool).unwrap());
        }

        let tools = if tools_array.is_empty() {
            None
        } else {
            Some(tools_array)
        };

        // Convert tool_choice - Responses API uses flat format, not nested function object
        let tool_choice = req.tool_choice.as_ref().map(|choice| {
            use super::interface::ToolChoice;
            match choice {
                ToolChoice::Auto => serde_json::json!("auto"),
                ToolChoice::None => serde_json::json!("none"),
                ToolChoice::Required => serde_json::json!("required"),
                // Responses API format: {"type": "function", "name": "fn_name"}
                // NOT the Chat Completions format: {"type": "function", "function": {"name": "fn_name"}}
                ToolChoice::Tool { name } => serde_json::json!({
                    "type": "function",
                    "name": name
                }),
            }
        });

        // Convert modalities
        let modalities = req.config.modalities.as_ref().map(|mods| {
            mods.iter()
                .map(|m| match m {
                    Modality::Text => "text",
                    Modality::Audio => "audio",
                    Modality::Image => "image",
                })
                .map(String::from)
                .collect()
        });

        // Convert reasoning effort
        let reasoning = req.config.reasoning_effort.as_ref().map(|effort| {
            let effort_str = match effort {
                ReasoningEffort::Minimal => "minimal",
                ReasoningEffort::Medium => "medium",
                ReasoningEffort::High => "high",
            };
            ReasoningConfig {
                effort: Some(effort_str.to_string()),
                summary: None,
            }
        });

        // Convert response format - Responses API uses text.format instead of response_format
        let text = match &req.config.response_format {
            super::interface::ResponseFormat::Text => None,
            super::interface::ResponseFormat::Json => Some(serde_json::json!({
                "format": {"type": "json_object"}
            })),
            super::interface::ResponseFormat::JsonSchema(schema) => Some(serde_json::json!({
                "format": {
                    "type": "json_schema",
                    "name": schema.name,
                    "schema": schema.schema,
                    "strict": schema.strict
                }
            })),
        };

        // Check if this is a reasoning model that doesn't support temperature
        // Reasoning models (gpt-5, o1, o3 series) don't support temperature, top_p parameters
        // Note: gpt-4o DOES support temperature, only gpt-5 and o-series don't
        let is_reasoning_model =
            model.starts_with("gpt-5") || model.starts_with("o1-") || model.starts_with("o3-");

        // Store responses when tools are provided (for agentic continuation)
        // or when explicitly continuing a conversation with previous_response_id.
        // This is required because previous_response_id only works if the original
        // response was stored.
        let should_store = !req.tools.is_empty() || req.previous_response_id.is_some();

        Self {
            model,
            input,
            instructions,
            previous_response_id: req.previous_response_id.clone(),
            store: Some(should_store),
            stream: None, // Set to Some(true) for streaming
            temperature: if is_reasoning_model {
                None
            } else {
                req.config.temperature
            },
            top_p: if is_reasoning_model {
                None
            } else {
                req.config.top_p
            },
            max_output_tokens: req.config.max_output_tokens,
            tools,
            tool_choice,
            modalities,
            reasoning,
            text,
        }
    }
}

// ============================================================================
// Responses API Response Types
// ============================================================================

/// Content item within a message
#[derive(Clone, Debug, Deserialize)]
pub struct ContentItem {
    #[serde(rename = "type")]
    pub content_type: String, // "output_text", "text", "image", etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// Output item from the Responses API
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum OutputItem {
    #[serde(rename = "message")]
    Message { content: Vec<ContentItem> },
    #[serde(rename = "function_call")]
    FunctionCall {
        arguments: String,
        call_id: String,
        name: String,
    },
    #[serde(rename = "tool_call")]
    ToolCall {
        #[serde(rename = "tool_name")]
        tool_name: String,
        arguments: Value,
    },
    #[serde(rename = "reasoning")]
    Reasoning {},
    #[serde(rename = "web_search_call")]
    WebSearchCall {
        id: String,
        #[serde(rename = "status", default)]
        _status: String,
        #[serde(flatten)]
        provider_fields: Map<String, Value>,
    },
    #[serde(rename = "code_interpreter_call")]
    CodeInterpreterCall {
        id: String,
        #[serde(rename = "status", default)]
        _status: String,
        #[serde(flatten)]
        provider_fields: Map<String, Value>,
    },
    #[serde(rename = "file_search_call")]
    FileSearchCall {
        id: String,
        #[serde(rename = "status", default)]
        _status: String,
        #[serde(flatten)]
        provider_fields: Map<String, Value>,
    },
    /// Future built-in tool action items (image_generation_call, mcp_call, ...).
    /// OpenAI emits these to describe tool actions it took; tolerate unknown
    /// variants rather than failing the whole response.
    #[serde(other)]
    Unknown,
}

/// Usage statistics from the API
#[derive(Clone, Debug, Deserialize)]
pub struct ApiUsage {
    #[serde(rename = "input_tokens")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u32>,
    #[serde(rename = "output_tokens")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u32>,
}

/// Complete Responses API response
#[derive(Clone, Debug, Deserialize)]
pub struct ResponsesApiResponse {
    pub id: String,
    pub created_at: i64,
    pub model: String,
    pub status: String,
    #[serde(default)]
    pub output: Vec<OutputItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<ApiUsage>,
    #[serde(default)]
    pub error: Option<Value>,
    #[serde(default)]
    pub incomplete_details: Option<Value>,
}

impl ResponsesApiResponse {
    fn status_error(&self) -> Option<SdkError> {
        if self.status == "completed" {
            return None;
        }

        let message = match self.status.as_str() {
            "failed" => self
                .error
                .as_ref()
                .and_then(extract_openai_error_message)
                .unwrap_or_else(|| "OpenAI Responses API response failed".to_string()),
            "incomplete" => {
                let details = self
                    .incomplete_details
                    .as_ref()
                    .and_then(extract_openai_error_message)
                    .unwrap_or_else(|| "unknown reason".to_string());
                format!("OpenAI Responses API response incomplete: {details}")
            }
            other => format!("OpenAI Responses API returned non-completed status `{other}`"),
        };

        Some(SdkError::LmApiError {
            status: 400,
            provider: "openai".to_string(),
            message,
            request_id: None,
        })
    }

    /// Convert to GenerateResponse (unified interface)
    pub fn into_generate_response(self) -> SdkResult<GenerateResponse> {
        if let Some(err) = self.status_error() {
            return Err(err);
        }

        // Extract text content from output items
        let mut text_parts = Vec::new();
        let tool_calls = tool_calls_from_output(&self.output);

        for item in &self.output {
            match item {
                OutputItem::Message { content, .. } => {
                    for content_item in content {
                        // Check for both "output_text" (Responses API) and "text" (legacy)
                        if content_item.content_type == "output_text"
                            || content_item.content_type == "text"
                        {
                            if let Some(text) = &content_item.text {
                                text_parts.push(text.clone());
                            }
                        }
                    }
                }
                OutputItem::FunctionCall { .. } | OutputItem::ToolCall { .. } => {}
                OutputItem::Reasoning { .. } => {
                    // Reasoning items from GPT-5 models are tracked separately in usage stats
                    // We don't include them in the text output
                }
                OutputItem::WebSearchCall { .. }
                | OutputItem::CodeInterpreterCall { .. }
                | OutputItem::FileSearchCall { .. } => {
                    // Built-in tool action items are surfaced through tool_calls.
                }
                OutputItem::Unknown => {
                    // Unknown output items carry no content we can surface.
                }
            }
        }

        let text = text_parts.join("\n");

        // Convert usage
        let usage = self.usage.map(|u| super::interface::TokenUsage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        });

        Ok(GenerateResponse {
            id: self.id,
            model: self.model,
            created: Some(self.created_at as u64),
            text,
            usage,
            finish_reason: Some(self.status),
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            object: None,
            raw: None,
            metadata: None,
        })
    }
}

fn tool_calls_from_output(output: &[OutputItem]) -> Vec<ToolCall> {
    let mut tool_calls = Vec::new();

    for item in output {
        match item {
            OutputItem::FunctionCall {
                call_id,
                name,
                arguments,
                ..
            } => {
                tool_calls.push(ToolCall {
                    id: call_id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                });
            }
            OutputItem::ToolCall {
                tool_name,
                arguments,
            } => {
                tool_calls.push(ToolCall {
                    id: format!("call_{}", tool_name),
                    name: tool_name.clone(),
                    arguments: arguments.to_string(),
                });
            }
            OutputItem::WebSearchCall {
                id,
                provider_fields,
                ..
            } => {
                tool_calls.push(ToolCall {
                    id: id.clone(),
                    name: "web_search_preview".to_string(),
                    arguments: built_in_tool_arguments(provider_fields),
                });
            }
            OutputItem::CodeInterpreterCall {
                id,
                provider_fields,
                ..
            } => {
                tool_calls.push(ToolCall {
                    id: id.clone(),
                    name: "code_interpreter".to_string(),
                    arguments: built_in_tool_arguments(provider_fields),
                });
            }
            OutputItem::FileSearchCall {
                id,
                provider_fields,
                ..
            } => {
                tool_calls.push(ToolCall {
                    id: id.clone(),
                    name: "file_search".to_string(),
                    arguments: built_in_tool_arguments(provider_fields),
                });
            }
            _ => {}
        }
    }

    tool_calls
}

fn built_in_tool_arguments(provider_fields: &Map<String, Value>) -> String {
    if provider_fields.is_empty() {
        "{}".to_string()
    } else {
        Value::Object(provider_fields.clone()).to_string()
    }
}

fn extract_openai_error_message(value: &Value) -> Option<String> {
    if let Some(message) = value.get("message").and_then(Value::as_str) {
        return Some(message.to_string());
    }

    if let Some(error) = value.get("error") {
        if let Some(message) = extract_openai_error_message(error) {
            return Some(message);
        }
    }

    if let Some(reason) = value.get("reason").and_then(Value::as_str) {
        return Some(reason.to_string());
    }

    if let Some(code) = value.get("code").and_then(Value::as_str) {
        return Some(code.to_string());
    }

    None
}

fn openai_streaming_error(data: &str) -> SdkError {
    let message = serde_json::from_str::<Value>(data)
        .ok()
        .and_then(|value| extract_openai_error_message(&value))
        .unwrap_or_else(|| data.to_string());

    SdkError::LmApiError {
        status: 400,
        provider: "openai".to_string(),
        message,
        request_id: None,
    }
}

// ============================================================================
// Responses API Streaming Types
// ============================================================================

/// SSE decoder for Responses API streaming format.
/// Handles the `event:` and `data:` line format used by the Responses API.
#[derive(Default)]
struct ResponsesSseDecoder {
    buffer: String,
}

impl ResponsesSseDecoder {
    /// Ingest raw bytes and return parsed SSE events.
    /// Each event is a tuple of (event_type, data_json).
    fn ingest(&mut self, chunk: &[u8]) -> SdkResult<Vec<(String, String)>> {
        let chunk_str = std::str::from_utf8(chunk)
            .map_err(|err| SdkError::Other(anyhow!("invalid UTF-8 in SSE stream: {err}")))?;
        self.buffer.push_str(chunk_str);

        let mut events = Vec::new();

        // Process complete events (separated by double newlines)
        while let Some(idx) = self.find_event_delimiter() {
            let (event_block, remaining) = self.buffer.split_at(idx);
            let delimiter_len = if remaining.starts_with("\r\n\r\n") {
                4
            } else {
                2
            };
            let event_block = event_block.to_string();
            self.buffer = remaining[delimiter_len..].to_string();

            // Parse event_type and data from the block
            let mut event_type = String::new();
            let mut data_parts = Vec::new();

            for line in event_block.lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    event_type = rest.trim().to_string();
                } else if let Some(rest) = line.strip_prefix("data:") {
                    data_parts.push(rest.trim_start().to_string());
                }
            }

            if !event_type.is_empty() || !data_parts.is_empty() {
                let data = data_parts.join("\n");
                events.push((event_type, data));
            }
        }

        Ok(events)
    }

    fn find_event_delimiter(&self) -> Option<usize> {
        self.buffer
            .find("\n\n")
            .or_else(|| self.buffer.find("\r\n\r\n"))
    }
}

/// Streaming delta event for text output
#[derive(Debug, Deserialize)]
struct OutputTextDelta {
    delta: String,
    #[allow(dead_code)]
    item_id: Option<String>,
    #[allow(dead_code)]
    output_index: Option<u32>,
    #[allow(dead_code)]
    content_index: Option<u32>,
}

/// Streaming delta event for reasoning/thinking output
#[derive(Debug, Deserialize)]
struct ReasoningTextDelta {
    delta: String,
    #[allow(dead_code)]
    item_id: Option<String>,
}

/// Response completed event with full response data
#[derive(Debug, Deserialize)]
struct ResponseCompletedEvent {
    response: ResponsesApiResponse,
}

/// Partial response state during streaming
#[derive(Default)]
struct StreamingState {
    text_started: bool,
    thinking_started: bool,
    text_aggregate: String,
    thinking_aggregate: String,
    response_id: Option<String>,
    model: Option<String>,
    created_at: Option<i64>,
    usage: Option<ApiUsage>,
    tool_calls: Vec<ToolCall>,
}

impl StreamingState {
    fn into_generate_response(
        self,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let text = self.text_aggregate;

        let object = match response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json | ResponseFormat::JsonSchema(_) => {
                if text.trim().is_empty() {
                    None
                } else {
                    Some(serde_json::from_str(text.trim()).map_err(|err| {
                        SdkError::Other(anyhow!("failed to parse JSON response: {err}"))
                    })?)
                }
            }
        };

        Ok(GenerateResponse {
            id: self.response_id.unwrap_or_default(),
            model: self.model.unwrap_or_default(),
            created: self.created_at.map(|t| t as u64),
            text,
            usage: self.usage.map(|u| TokenUsage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
                total_tokens: u.total_tokens,
            }),
            finish_reason: Some("completed".to_string()),
            tool_calls: if self.tool_calls.is_empty() {
                None
            } else {
                Some(self.tool_calls)
            },
            object,
            raw: None,
            metadata: None,
        })
    }
}

/// Build a streaming response from the Responses API SSE format.
fn build_responses_stream(
    response: reqwest::Response,
    response_format: ResponseFormat,
    _timeout_secs: u64,
) -> SdkResult<Pin<Box<dyn Stream<Item = SdkResult<StreamChunk>> + Send>>> {
    let bytes_stream = response.bytes_stream();

    let stream = try_stream! {
        futures::pin_mut!(bytes_stream);
        let mut decoder = ResponsesSseDecoder::default();
        let mut state = StreamingState::default();

        while let Some(chunk) = bytes_stream.next().await {
            let chunk = chunk.map_err(|err| http::classify_reqwest_error(err, "openai"))?;

            for (event_type, data) in decoder.ingest(chunk.as_ref())? {
                if data.is_empty() {
                    continue;
                }

                match event_type.as_str() {
                    // Response lifecycle events
                    "response.created" => {
                        // Extract response metadata
                        if let Ok(resp) = serde_json::from_str::<ResponsesApiResponse>(&data) {
                            state.response_id = Some(resp.id);
                            state.model = Some(resp.model);
                            state.created_at = Some(resp.created_at);
                        }
                    }

                    // Text output delta
                    "response.output_text.delta" => {
                        if let Ok(delta) = serde_json::from_str::<OutputTextDelta>(&data) {
                            if !delta.delta.is_empty() {
                                // Emit ContentBlockStart on first text content
                                if !state.text_started {
                                    yield StreamChunk::ContentBlockStart {
                                        index: 0,
                                        block_type: ContentBlockType::Text,
                                    };
                                    state.text_started = true;
                                }
                                state.text_aggregate.push_str(&delta.delta);
                                yield StreamChunk::Delta {
                                    content: delta.delta,
                                    index: 0,
                                    block_type: ContentBlockType::Text,
                                };
                            }
                        }
                    }

                    // Text output done - mark as closed but don't emit stop yet
                    // (we emit all stops together in response.completed)
                    "response.output_text.done" => {
                        // Text block is done, but we wait for response.completed to emit stop
                    }

                    // Reasoning/thinking delta (for o1, o3, gpt-5 models)
                    "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                        if let Ok(delta) = serde_json::from_str::<ReasoningTextDelta>(&data) {
                            if !delta.delta.is_empty() {
                                // Emit ContentBlockStart on first thinking content
                                if !state.thinking_started {
                                    yield StreamChunk::ContentBlockStart {
                                        index: 1,
                                        block_type: ContentBlockType::Thinking,
                                    };
                                    state.thinking_started = true;
                                }
                                state.thinking_aggregate.push_str(&delta.delta);
                                yield StreamChunk::Delta {
                                    content: delta.delta,
                                    index: 1,
                                    block_type: ContentBlockType::Thinking,
                                };
                            }
                        }
                    }

                    // Reasoning done - mark as closed but don't emit stop yet
                    "response.reasoning_summary_text.done" | "response.reasoning_text.done" => {
                        // Thinking block is done, but we wait for response.completed to emit stop
                    }

                    // Response completed - final event
                    "response.completed" => {
                        if let Ok(completed) = serde_json::from_str::<ResponseCompletedEvent>(&data) {
                            let tool_calls = tool_calls_from_output(&completed.response.output);
                            state.usage = completed.response.usage;
                            state.response_id = Some(completed.response.id);
                            state.model = Some(completed.response.model);
                            state.tool_calls = tool_calls;
                        }

                        // Close any open content blocks
                        if state.text_started {
                            yield StreamChunk::ContentBlockStop { index: 0 };
                        }
                        if state.thinking_started {
                            yield StreamChunk::ContentBlockStop { index: 1 };
                        }

                        let response = state.into_generate_response(response_format)?;
                        yield StreamChunk::Completed(response);
                        return;
                    }

                    "response.failed" | "response.incomplete" => {
                        let event = serde_json::from_str::<ResponseCompletedEvent>(&data)
                            .map_err(|err| {
                                SdkError::Other(anyhow!(
                                    "failed to parse OpenAI Responses failure event: {err}"
                                ))
                            })?;

                        if let Some(err) = event.response.status_error() {
                            Err(err)?;
                        }

                        Err(SdkError::LmApiError {
                            status: 400,
                            provider: "openai".to_string(),
                            message: format!(
                                "OpenAI Responses API returned {} event without error details",
                                event_type
                            ),
                            request_id: None,
                        })?;
                    }

                    // Error event
                    "error" => {
                        Err(openai_streaming_error(&data))?;
                    }

                    // Ignore other events (response.in_progress, response.output_item.added, etc.)
                    _ => {}
                }
            }
        }

        // If we get here without a response.completed event, create response from state
        if state.text_started || state.thinking_started {
            let response = state.into_generate_response(response_format)?;
            yield StreamChunk::Completed(response);
        } else {
            Err(SdkError::Other(anyhow!("stream ended without response data")))?;
        }
    };

    Ok(Box::pin(stream))
}

/// Create a StreamHandle from a Responses API streaming response.
fn responses_stream_handle(
    response: reqwest::Response,
    response_format: ResponseFormat,
    timeout_secs: u64,
) -> SdkResult<StreamHandle> {
    let stream = build_responses_stream(response, response_format, timeout_secs)?;
    Ok(StreamHandle::new(stream))
}

// ============================================================================
// Error Handling Helpers
// ============================================================================

// ============================================================================
// LanguageModel Trait Implementation
// ============================================================================

fn validate_request(request: &GenerateRequest) -> SdkResult<()> {
    if request.system_prompt.is_none() && request.messages.is_empty() {
        return Err(SdkError::Configuration {
            message:
                "at least a system prompt or one message is required for OpenAI Responses requests"
                    .to_string(),
            field: None,
        });
    }
    Ok(())
}

#[async_trait]
impl LanguageModel for OpenAiProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        // Create OpenTelemetry span for this LLM call as child of the current execution span
        // request.otel_context is populated by the Python SDK with the current span context
        // (e.g., python_component_execution) to ensure proper parent-child relationships
        let mut span =
            telemetry::create_gen_ai_span("openai", &request.model, request.otel_context.clone());

        // Set request configuration attributes
        telemetry::set_request_attributes(&mut span, &request);

        // Optional content capture (enabled by default)
        let capture_content = telemetry::should_capture_content();

        // Capture tool definitions and tool choice
        telemetry::set_tool_request_attributes(&mut span, &request, capture_content);

        if capture_content {
            // Capture system instructions separately per OpenTelemetry spec
            if let Some(system_prompt) = &request.system_prompt {
                let system_instructions = telemetry::serialize_system_instructions(system_prompt);
                span.set_attribute(opentelemetry::KeyValue::new(
                    telemetry::attributes::SYSTEM_INSTRUCTIONS,
                    system_instructions.to_string(),
                ));
            }

            // Capture conversation messages (without system instructions)
            let input_messages = telemetry::serialize_input_messages(&request);
            span.set_attribute(opentelemetry::KeyValue::new(
                telemetry::attributes::INPUT_MESSAGES,
                input_messages.to_string(),
            ));
        }

        // Track request start time for latency measurement
        let start = std::time::Instant::now();

        // Execute the actual API call
        let result: SdkResult<GenerateResponse> = async {
            validate_request(&request)?;
            let model = self.normalize_model(&request.model)?;
            let payload = ResponsesApiRequest::from_request(&request, model);

            let response = http::send_with_retry(
                || self.request().json(&payload),
                &self.config.retry_config,
                "openai",
                request.config.timeout,
            )
            .await?;

            let metadata = http::extract_metadata(&response);

            // Get response text for debugging
            let response_text = response
                .text()
                .await
                .map_err(|err| http::classify_reqwest_error(err, "openai"))?;

            tracing::debug!("OpenAI Responses API raw response: {}", response_text);

            let parsed: ResponsesApiResponse =
                serde_json::from_str(&response_text).map_err(|err| {
                    tracing::error!(
                        "Failed to parse OpenAI Responses response. Error: {}, Response body: {}",
                        err,
                        response_text
                    );
                    SdkError::Other(anyhow!("failed to parse OpenAI Responses response: {err}"))
                })?;

            let mut result = parsed.into_generate_response()?;
            result.metadata = Some(metadata);
            Ok(result)
        }
        .await;

        // Record latency on the span
        let duration_ms = start.elapsed().as_millis();
        telemetry::set_duration(&mut span, duration_ms);

        // Handle result and set span attributes
        match result {
            Ok(response) => {
                telemetry::set_response_attributes(&mut span, &response, capture_content);

                // Calculate and set cost if token usage is available
                if let Some(usage) = &response.usage {
                    if let (Some(input_tokens), Some(output_tokens)) =
                        (usage.prompt_tokens, usage.completion_tokens)
                    {
                        // TODO: Extract cached tokens when TokenUsage struct is extended
                        // For now, calculate cost without cache discount
                        if let Some(cost) = telemetry::calculate_cost(
                            "openai",
                            &response.model,
                            input_tokens as u32,
                            output_tokens as u32,
                            None, // cached_tokens - will be added when TokenUsage is extended
                        ) {
                            telemetry::set_cost_attributes(&mut span, cost);
                        }
                    }
                }

                span.end();
                Ok(response)
            }
            Err(err) => {
                telemetry::set_error_status(&mut span, &err.to_string());
                span.end();
                Err(err)
            }
        }
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        // Create OpenTelemetry span for this streaming LLM call
        // request.otel_context contains the current execution span context
        let mut span =
            telemetry::create_gen_ai_span("openai", &request.model, request.otel_context.clone());

        // Set request configuration attributes
        telemetry::set_request_attributes(&mut span, &request);

        // Mark as streaming
        span.set_attribute(opentelemetry::KeyValue::new("llm.streaming", true));

        // Optional content capture (enabled by default)
        let capture_content = telemetry::should_capture_content();

        // Capture tool definitions and tool choice
        telemetry::set_tool_request_attributes(&mut span, &request, capture_content);

        if capture_content {
            // Capture system instructions separately per OpenTelemetry spec
            if let Some(system_prompt) = &request.system_prompt {
                let system_instructions = telemetry::serialize_system_instructions(system_prompt);
                span.set_attribute(opentelemetry::KeyValue::new(
                    telemetry::attributes::SYSTEM_INSTRUCTIONS,
                    system_instructions.to_string(),
                ));
            }

            // Capture conversation messages (without system instructions)
            let input_messages = telemetry::serialize_input_messages(&request);
            span.set_attribute(opentelemetry::KeyValue::new(
                telemetry::attributes::INPUT_MESSAGES,
                input_messages.to_string(),
            ));
        }

        // Track request start time
        let start = std::time::Instant::now();

        // Execute the actual streaming API call using the Responses API
        let result = async {
            validate_request(&request)?;
            let model = self.normalize_model(&request.model)?;

            // Build Responses API request with streaming enabled
            let mut payload = ResponsesApiRequest::from_request(&request, model);
            payload.stream = Some(true);

            let response = http::send_with_retry(
                || {
                    self.request()
                        .header("Accept", "text/event-stream")
                        .json(&payload)
                },
                &self.config.retry_config,
                "openai",
                request.config.timeout,
            )
            .await?;
            responses_stream_handle(
                response,
                request.config.response_format.clone(),
                self.config.timeout.as_secs(),
            )
        }
        .await;

        // Record latency for stream initialization on the span
        let duration_ms = start.elapsed().as_millis();
        telemetry::set_duration(&mut span, duration_ms);

        // Handle result
        // Note: For streaming, we end the span immediately after stream starts
        // Individual chunks are not traced separately in this implementation
        match result {
            Ok(stream_handle) => {
                span.set_status(opentelemetry::trace::Status::Ok);
                span.end();
                Ok(stream_handle)
            }
            Err(err) => {
                telemetry::set_error_status(&mut span, &err.to_string());
                span.end();
                Err(err)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // ResponsesSseDecoder Tests
    // ========================================================================

    #[test]
    fn test_sse_decoder_basic_event() {
        let mut decoder = ResponsesSseDecoder::default();
        let chunk = b"event: response.created\ndata: {\"id\": \"resp_123\"}\n\n";

        let events = decoder.ingest(chunk).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "response.created");
        assert_eq!(events[0].1, "{\"id\": \"resp_123\"}");
    }

    #[test]
    fn test_sse_decoder_multiple_events() {
        let mut decoder = ResponsesSseDecoder::default();
        let chunk = b"event: response.output_text.delta\ndata: {\"delta\": \"Hello\"}\n\nevent: response.output_text.delta\ndata: {\"delta\": \" world\"}\n\n";

        let events = decoder.ingest(chunk).unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, "response.output_text.delta");
        assert_eq!(events[0].1, "{\"delta\": \"Hello\"}");
        assert_eq!(events[1].0, "response.output_text.delta");
        assert_eq!(events[1].1, "{\"delta\": \" world\"}");
    }

    #[test]
    fn test_sse_decoder_chunked_input() {
        let mut decoder = ResponsesSseDecoder::default();

        // First chunk - incomplete event
        let chunk1 = b"event: response.output_text.delta\ndata: {\"del";
        let events1 = decoder.ingest(chunk1).unwrap();
        assert_eq!(events1.len(), 0); // No complete events yet

        // Second chunk - completes the event
        let chunk2 = b"ta\": \"Hello\"}\n\n";
        let events2 = decoder.ingest(chunk2).unwrap();
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].0, "response.output_text.delta");
        assert_eq!(events2[0].1, "{\"delta\": \"Hello\"}");
    }

    #[test]
    fn test_sse_decoder_crlf_delimiter() {
        let mut decoder = ResponsesSseDecoder::default();
        let chunk = b"event: response.created\r\ndata: {\"id\": \"resp_123\"}\r\n\r\n";

        let events = decoder.ingest(chunk).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "response.created");
    }

    #[test]
    fn test_sse_decoder_empty_data() {
        let mut decoder = ResponsesSseDecoder::default();
        let chunk = b"event: response.in_progress\ndata: \n\n";

        let events = decoder.ingest(chunk).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "response.in_progress");
        assert_eq!(events[0].1, "");
    }

    #[test]
    fn test_sse_decoder_multiline_data() {
        let mut decoder = ResponsesSseDecoder::default();
        let chunk =
            b"event: response.completed\ndata: {\"response\":\ndata:  {\"id\": \"123\"}}\n\n";

        let events = decoder.ingest(chunk).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "response.completed");
        // Multiline data should be joined
        assert!(events[0].1.contains("{\"response\":"));
    }

    // ========================================================================
    // OutputTextDelta Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_output_text_delta() {
        let json = r#"{"delta": "Hello, world!", "item_id": "item_123", "output_index": 0, "content_index": 0}"#;
        let delta: OutputTextDelta = serde_json::from_str(json).unwrap();

        assert_eq!(delta.delta, "Hello, world!");
        assert_eq!(delta.item_id, Some("item_123".to_string()));
        assert_eq!(delta.output_index, Some(0));
        assert_eq!(delta.content_index, Some(0));
    }

    #[test]
    fn test_parse_output_text_delta_minimal() {
        let json = r#"{"delta": "Test"}"#;
        let delta: OutputTextDelta = serde_json::from_str(json).unwrap();

        assert_eq!(delta.delta, "Test");
        assert_eq!(delta.item_id, None);
    }

    // ========================================================================
    // ReasoningTextDelta Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_reasoning_text_delta() {
        let json = r#"{"delta": "Let me think about this...", "item_id": "reasoning_123"}"#;
        let delta: ReasoningTextDelta = serde_json::from_str(json).unwrap();

        assert_eq!(delta.delta, "Let me think about this...");
        assert_eq!(delta.item_id, Some("reasoning_123".to_string()));
    }

    // ========================================================================
    // ResponseCompletedEvent Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_response_completed_event() {
        let json = r#"{
            "response": {
                "id": "resp_abc123",
                "created_at": 1700000000,
                "model": "gpt-4o-mini",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": "Hello!"}
                        ]
                    }
                ],
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "total_tokens": 15
                }
            }
        }"#;

        let event: ResponseCompletedEvent = serde_json::from_str(json).unwrap();

        assert_eq!(event.response.id, "resp_abc123");
        assert_eq!(event.response.model, "gpt-4o-mini");
        assert_eq!(event.response.status, "completed");
        assert!(event.response.usage.is_some());
        let usage = event.response.usage.unwrap();
        assert_eq!(usage.prompt_tokens, Some(10));
        assert_eq!(usage.completion_tokens, Some(5));
    }

    #[test]
    fn test_failed_response_becomes_lm_api_error() {
        let json = r#"{
            "id": "resp_failed",
            "created_at": 1700000000,
            "model": "gpt-10-mini",
            "status": "failed",
            "error": {
                "code": "model_not_found",
                "message": "The requested model 'gpt-10-mini' does not exist."
            }
        }"#;

        let response: ResponsesApiResponse = serde_json::from_str(json).unwrap();
        let err = response.into_generate_response().unwrap_err();

        match err {
            SdkError::LmApiError {
                status,
                provider,
                message,
                ..
            } => {
                assert_eq!(status, 400);
                assert_eq!(provider, "openai");
                assert!(message.contains("gpt-10-mini"));
            }
            other => panic!("expected LmApiError, got {other:?}"),
        }
    }

    #[test]
    fn test_incomplete_response_becomes_lm_api_error() {
        let json = r#"{
            "id": "resp_incomplete",
            "created_at": 1700000000,
            "model": "gpt-4o-mini",
            "status": "incomplete",
            "incomplete_details": {
                "reason": "max_output_tokens"
            }
        }"#;

        let response: ResponsesApiResponse = serde_json::from_str(json).unwrap();
        let err = response.into_generate_response().unwrap_err();

        match err {
            SdkError::LmApiError { message, .. } => {
                assert!(message.contains("incomplete"));
                assert!(message.contains("max_output_tokens"));
            }
            other => panic!("expected LmApiError, got {other:?}"),
        }
    }

    #[test]
    fn test_response_with_builtin_tool_output_items() {
        // OpenAI emits web_search_call (and other built-in tool action items)
        // alongside the message. Known built-ins should surface as tool calls,
        // while future unknown item types must not fail parsing.
        let json = r#"{
            "id": "resp_websearch",
            "created_at": 1700000000,
            "model": "gpt-4o-mini",
            "status": "completed",
            "output": [
                {
                    "type": "web_search_call",
                    "id": "ws_1",
                    "status": "completed",
                    "action": {"type": "search", "query": "AGNT5 built-in tools"}
                },
                {
                    "type": "code_interpreter_call",
                    "id": "ci_1",
                    "status": "completed",
                    "code": "print('hello')"
                },
                {
                    "type": "file_search_call",
                    "id": "fs_1",
                    "status": "completed",
                    "queries": ["AGNT5"],
                    "results": [{"file_id": "file_1"}]
                },
                {"type": "image_generation_call", "id": "ig_1", "status": "completed"},
                {"type": "message", "content": [{"type": "output_text", "text": "Done."}]}
            ]
        }"#;

        let response: ResponsesApiResponse = serde_json::from_str(json).unwrap();
        let generated = response.into_generate_response().unwrap();
        assert_eq!(generated.text.trim(), "Done.");

        let tool_calls = generated.tool_calls.unwrap();
        assert_eq!(tool_calls.len(), 3);
        assert_eq!(tool_calls[0].id, "ws_1");
        assert_eq!(tool_calls[0].name, "web_search_preview");
        assert_eq!(
            serde_json::from_str::<Value>(&tool_calls[0].arguments).unwrap(),
            serde_json::json!({"action": {"type": "search", "query": "AGNT5 built-in tools"}})
        );
        assert_eq!(tool_calls[1].id, "ci_1");
        assert_eq!(tool_calls[1].name, "code_interpreter");
        assert_eq!(
            serde_json::from_str::<Value>(&tool_calls[1].arguments).unwrap(),
            serde_json::json!({"code": "print('hello')"})
        );
        assert_eq!(tool_calls[2].id, "fs_1");
        assert_eq!(tool_calls[2].name, "file_search");
        assert_eq!(
            serde_json::from_str::<Value>(&tool_calls[2].arguments).unwrap(),
            serde_json::json!({
                "queries": ["AGNT5"],
                "results": [{"file_id": "file_1"}]
            })
        );
    }

    #[test]
    fn test_openai_streaming_error_extracts_message() {
        let err = openai_streaming_error(
            r#"{"error": {"message": "The requested model does not exist."}}"#,
        );

        match err {
            SdkError::LmApiError { message, .. } => {
                assert_eq!(message, "The requested model does not exist.");
            }
            other => panic!("expected LmApiError, got {other:?}"),
        }
    }

    // ========================================================================
    // StreamingState Tests
    // ========================================================================

    #[test]
    fn test_streaming_state_text_response() {
        let mut state = StreamingState::default();
        state.text_started = true;
        state.text_aggregate = "Hello, world!".to_string();
        state.response_id = Some("resp_123".to_string());
        state.model = Some("gpt-4o-mini".to_string());
        state.created_at = Some(1700000000);
        state.usage = Some(ApiUsage {
            prompt_tokens: Some(10),
            completion_tokens: Some(5),
            total_tokens: Some(15),
        });

        let response = state.into_generate_response(ResponseFormat::Text).unwrap();

        assert_eq!(response.id, "resp_123");
        assert_eq!(response.model, "gpt-4o-mini");
        assert_eq!(response.text, "Hello, world!");
        assert_eq!(response.finish_reason, Some("completed".to_string()));
        assert!(response.usage.is_some());
        let usage = response.usage.unwrap();
        assert_eq!(usage.prompt_tokens, Some(10));
        assert_eq!(usage.completion_tokens, Some(5));
    }

    #[test]
    fn test_streaming_state_preserves_tool_calls() {
        let mut state = StreamingState::default();
        state.response_id = Some("resp_tools".to_string());
        state.model = Some("gpt-4o-mini".to_string());
        state.tool_calls = vec![ToolCall {
            id: "call_123".to_string(),
            name: "lookup_weather".to_string(),
            arguments: "{\"city\":\"SF\"}".to_string(),
        }];

        let response = state.into_generate_response(ResponseFormat::Text).unwrap();
        let tool_calls = response.tool_calls.unwrap();

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "call_123");
        assert_eq!(tool_calls[0].name, "lookup_weather");
        assert_eq!(tool_calls[0].arguments, "{\"city\":\"SF\"}");
    }

    #[test]
    fn test_streaming_state_json_response() {
        let mut state = StreamingState::default();
        state.text_started = true;
        state.text_aggregate = r#"{"name": "test", "value": 42}"#.to_string();
        state.response_id = Some("resp_456".to_string());
        state.model = Some("gpt-4o".to_string());

        let response = state.into_generate_response(ResponseFormat::Json).unwrap();

        assert_eq!(response.text, r#"{"name": "test", "value": 42}"#);
        assert!(response.object.is_some());
        let obj = response.object.unwrap();
        assert_eq!(obj["name"], "test");
        assert_eq!(obj["value"], 42);
    }

    #[test]
    fn test_streaming_state_empty_defaults() {
        let state = StreamingState::default();
        let response = state.into_generate_response(ResponseFormat::Text).unwrap();

        assert_eq!(response.id, "");
        assert_eq!(response.model, "");
        assert_eq!(response.text, "");
        assert!(response.usage.is_none());
    }

    // ========================================================================
    // ResponsesApiRequest Tests
    // ========================================================================

    #[test]
    fn test_responses_api_request_simple() {
        let request = GenerateRequest::new("openai/gpt-4o-mini")
            .system_prompt("You are a helpful assistant.")
            .user_message("Hello");

        let payload = ResponsesApiRequest::from_request(&request, "gpt-4o-mini".to_string());

        assert_eq!(payload.model, "gpt-4o-mini");
        assert_eq!(
            payload.instructions,
            Some("You are a helpful assistant.".to_string())
        );
        assert_eq!(payload.store, Some(false));
        assert!(payload.stream.is_none()); // Not set by from_request
    }

    #[test]
    fn test_responses_api_request_with_streaming() {
        let request = GenerateRequest::new("openai/gpt-4o-mini")
            .system_prompt("Test")
            .user_message("Hi");

        let mut payload = ResponsesApiRequest::from_request(&request, "gpt-4o-mini".to_string());
        payload.stream = Some(true);

        assert_eq!(payload.stream, Some(true));
    }

    #[test]
    fn test_responses_api_request_reasoning_model() {
        let request = GenerateRequest::new("openai/gpt-5")
            .system_prompt("Test")
            .user_message("Hi")
            .configure(|c| {
                c.temperature = Some(0.7);
                c.top_p = Some(0.9);
            });

        let payload = ResponsesApiRequest::from_request(&request, "gpt-5".to_string());

        // Reasoning models should NOT have temperature or top_p
        assert!(payload.temperature.is_none());
        assert!(payload.top_p.is_none());
    }

    #[test]
    fn test_responses_api_request_non_reasoning_model() {
        let request = GenerateRequest::new("openai/gpt-4o")
            .system_prompt("Test")
            .user_message("Hi")
            .configure(|c| {
                c.temperature = Some(0.7);
                c.top_p = Some(0.9);
            });

        let payload = ResponsesApiRequest::from_request(&request, "gpt-4o".to_string());

        // Non-reasoning models SHOULD have temperature and top_p
        assert_eq!(payload.temperature, Some(0.7));
        assert_eq!(payload.top_p, Some(0.9));
    }

    // ========================================================================
    // Model Normalization Tests
    // ========================================================================

    #[test]
    fn test_normalize_model_with_prefix() {
        let config = OpenAiConfig::new("test-key");
        let provider = OpenAiProvider::new(config).unwrap();

        let result = provider.normalize_model("openai/gpt-4o-mini");
        assert_eq!(result.unwrap(), "gpt-4o-mini");
    }

    #[test]
    fn test_normalize_model_wrong_prefix() {
        let config = OpenAiConfig::new("test-key");
        let provider = OpenAiProvider::new(config).unwrap();

        let result = provider.normalize_model("anthropic/claude-3");
        assert!(result.is_err());
    }

    #[test]
    fn test_normalize_model_no_prefix_required() {
        let config = OpenAiConfig::new("test-key").with_model_prefix(None::<String>);
        let provider = OpenAiProvider::new(config).unwrap();

        let result = provider.normalize_model("gpt-4o-mini");
        assert_eq!(result.unwrap(), "gpt-4o-mini");
    }

    #[test]
    fn test_normalize_model_empty() {
        let config = OpenAiConfig::new("test-key");
        let provider = OpenAiProvider::new(config).unwrap();

        let result = provider.normalize_model("");
        assert!(result.is_err());
    }
}
