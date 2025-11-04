use std::env;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use opentelemetry::trace::Span;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    generate as generate_via_model, stream as stream_via_model, BuiltInTool, GenerateRequest,
    GenerateResponse, LanguageModel, Modality, ReasoningEffort, StreamHandle, StreamRequest,
};
use super::openai_common::{parse_error, stream_handle_from_response};
use super::telemetry;

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_RESPONSES_PATH: &str = "responses";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
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
        let api_key = env::var("OPENAI_API_KEY")
            .map_err(|_| SdkError::Configuration("OPENAI_API_KEY must be set".to_string()))?;

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
        let http = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|err| SdkError::Other(anyhow!("failed to construct HTTP client: {err}")))?;

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
            return Err(SdkError::Configuration(
                "model id must not be empty for OpenAI Responses requests".to_string(),
            ));
        }

        match &self.config.model_prefix {
            Some(prefix) => {
                if let Some((provider, rest)) = trimmed.split_once('/') {
                    let rest = rest.trim();
                    if provider != prefix {
                        return Err(SdkError::Configuration(format!(
                            "expected model prefix `{prefix}/`, got `{provider}`"
                        )));
                    }
                    if rest.is_empty() {
                        return Err(SdkError::Configuration(format!(
                            "model id must follow `{prefix}/` prefix"
                        )));
                    }
                    Ok(rest.to_string())
                } else {
                    Err(SdkError::Configuration(format!(
                        "model should be prefixed with `{prefix}/`"
                    )))
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

/// Input for the Responses API - can be simple text or structured messages
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum InputType {
    /// Simple string input
    Simple(String),
    /// Structured message array
    Messages(Vec<ApiMessage>),
}

/// Message format for Responses API
#[derive(Clone, Debug, Serialize)]
pub struct ApiMessage {
    pub role: String,
    pub content: String,
}

impl From<&super::interface::Message> for ApiMessage {
    fn from(msg: &super::interface::Message) -> Self {
        Self {
            role: match msg.role {
                super::interface::MessageRole::User => "user".to_string(),
                super::interface::MessageRole::Assistant => "assistant".to_string(),
                super::interface::MessageRole::System => "system".to_string(),
            },
            content: msg.content.clone(),
        }
    }
}

/// Reasoning configuration for o-series models
#[derive(Clone, Debug, Serialize)]
pub struct ReasoningConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>, // "minimal", "medium", "high"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<bool>,
}

/// API tool definition (user-defined tools, not built-in)
#[derive(Clone, Debug, Serialize)]
pub struct ApiTool {
    #[serde(rename = "type")]
    pub tool_type: String, // "function"
    pub function: ApiFunction,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiFunction {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<Value>,
}

impl ResponsesApiRequest {
    pub fn from_request(req: &GenerateRequest, model: String) -> Self {
        // Build input
        let input = if req.messages.is_empty() {
            // Simple string input from system prompt
            InputType::Simple(req.system_prompt.clone().unwrap_or_default())
        } else {
            // Message array
            InputType::Messages(req.messages.iter().map(ApiMessage::from).collect())
        };

        // System prompt becomes instructions (separate from messages)
        let instructions = req.system_prompt.clone();

        // Build tools array (mix of user-defined and built-in)
        let mut tools_array: Vec<Value> = Vec::new();

        // Add user-defined function tools
        for tool in &req.tools {
            let api_tool = ApiTool {
                tool_type: "function".to_string(),
                function: ApiFunction {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    parameters: tool.parameters.clone().unwrap_or(serde_json::json!({})),
                    strict: tool.strict,
                },
            };
            tools_array.push(serde_json::to_value(api_tool).unwrap());
        }

        // Add built-in tools
        for built_in_tool in &req.config.built_in_tools {
            let tool_type = match built_in_tool {
                BuiltInTool::WebSearch => "web_search_preview",
                BuiltInTool::CodeInterpreter => "code_interpreter",
                BuiltInTool::FileSearch => "file_search",
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

        // Convert tool_choice
        let tool_choice = req.tool_choice.as_ref().map(|choice| {
            use super::interface::ToolChoice;
            match choice {
                ToolChoice::Auto => serde_json::json!("auto"),
                ToolChoice::None => serde_json::json!("none"),
                ToolChoice::Tool { name } => serde_json::json!({
                    "type": "function",
                    "function": {"name": name}
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

        // Convert response format
        let response_format = match &req.config.response_format {
            super::interface::ResponseFormat::Text => None,
            super::interface::ResponseFormat::Json => Some(serde_json::json!({"type": "json_object"})),
            super::interface::ResponseFormat::JsonSchema(schema) => Some(serde_json::json!({
                "type": "json_schema",
                "json_schema": {
                    "name": schema.name,
                    "schema": schema.schema,
                    "strict": schema.strict
                }
            })),
        };

        Self {
            model,
            input,
            instructions,
            previous_response_id: None,
            store: Some(false), // Stateless by default
            temperature: req.config.temperature,
            top_p: req.config.top_p,
            max_output_tokens: req.config.max_output_tokens,
            tools,
            tool_choice,
            modalities,
            reasoning,
            response_format,
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
    Message {
        id: String,
        status: String,
        role: String,
        content: Vec<ContentItem>,
    },
    #[serde(rename = "tool_call")]
    ToolCall {
        #[serde(rename = "tool_name")]
        tool_name: String,
        arguments: Value,
    },
    #[serde(rename = "reasoning")]
    Reasoning {
        id: String,
        summary: Vec<Value>, // Summary can be empty or contain reasoning steps
    },
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
    pub object: String,
    pub created_at: i64,
    pub model: String,
    pub status: String,
    pub output: Vec<OutputItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<ApiUsage>,
}

impl ResponsesApiResponse {
    /// Convert to GenerateResponse (unified interface)
    pub fn into_generate_response(self) -> SdkResult<GenerateResponse> {
        // Extract text content from output items
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();

        for item in &self.output {
            match item {
                OutputItem::Message { content, .. } => {
                    for content_item in content {
                        // Check for both "output_text" (Responses API) and "text" (legacy)
                        if content_item.content_type == "output_text" || content_item.content_type == "text" {
                            if let Some(text) = &content_item.text {
                                text_parts.push(text.clone());
                            }
                        }
                    }
                }
                OutputItem::ToolCall {
                    tool_name,
                    arguments,
                } => {
                    tool_calls.push(super::interface::ToolCall {
                        id: format!("call_{}", tool_name), // Generate a simple ID
                        name: tool_name.clone(),
                        arguments: arguments.to_string(),
                    });
                }
                OutputItem::Reasoning { .. } => {
                    // Reasoning items from GPT-5 models are tracked separately in usage stats
                    // We don't include them in the text output
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
        })
    }
}

// ============================================================================
// Streaming Event Types
// ============================================================================

/// Streaming events from Responses API
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ResponseEvent {
    #[serde(rename = "response.created")]
    ResponseCreated {
        #[serde(flatten)]
        data: ResponseCreatedData,
    },
    #[serde(rename = "response.output_item.added")]
    OutputItemAdded {
        #[serde(flatten)]
        data: OutputItemAddedData,
    },
    #[serde(rename = "response.output_text.delta")]
    OutputTextDelta {
        #[serde(flatten)]
        data: OutputTextDeltaData,
    },
    #[serde(rename = "response.output_text.done")]
    OutputTextDone {
        #[serde(flatten)]
        data: OutputTextDoneData,
    },
    #[serde(rename = "response.completed")]
    Completed {
        #[serde(flatten)]
        data: ResponseCompletedData,
    },
    #[serde(rename = "error")]
    Error {
        error: ErrorData,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct ResponseCreatedData {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OutputItemAddedData {
    pub item: OutputItem,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OutputTextDeltaData {
    pub delta: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OutputTextDoneData {
    pub text: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ResponseCompletedData {
    pub response: ResponsesApiResponse,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ErrorData {
    pub message: String,
}

/// Accumulator for streaming events
pub struct EventAccumulator {
    pub id: Option<String>,
    pub model: Option<String>,
    pub status: Option<String>,
    pub accumulated_text: String,
    pub output_items: Vec<OutputItem>,
    pub usage: Option<ApiUsage>,
}

impl EventAccumulator {
    pub fn new() -> Self {
        Self {
            id: None,
            model: None,
            status: None,
            accumulated_text: String::new(),
            output_items: Vec::new(),
            usage: None,
        }
    }

    pub fn update(&mut self, event: ResponseEvent) {
        match event {
            ResponseEvent::ResponseCreated { data } => {
                self.id = Some(data.id);
            }
            ResponseEvent::OutputItemAdded { data } => {
                self.output_items.push(data.item);
            }
            ResponseEvent::OutputTextDelta { data } => {
                self.accumulated_text.push_str(&data.delta);
            }
            ResponseEvent::OutputTextDone { data } => {
                self.accumulated_text = data.text;
            }
            ResponseEvent::Completed { data } => {
                self.id = Some(data.response.id);
                self.model = Some(data.response.model);
                self.status = Some(data.response.status);
                self.output_items = data.response.output;
                self.usage = data.response.usage;
            }
            ResponseEvent::Error { .. } => {
                // Error handling done at a higher level
            }
        }
    }

    pub fn into_generate_response(self) -> SdkResult<GenerateResponse> {
        let id = self.id.ok_or_else(|| {
            SdkError::Other(anyhow!("missing response id in streaming response"))
        })?;
        let model = self.model.ok_or_else(|| {
            SdkError::Other(anyhow!("missing model in streaming response"))
        })?;

        // Extract tool calls
        let mut tool_calls = Vec::new();
        for item in &self.output_items {
            if let OutputItem::ToolCall {
                tool_name,
                arguments,
            } = item
            {
                tool_calls.push(super::interface::ToolCall {
                    id: format!("call_{}", tool_name),
                    name: tool_name.clone(),
                    arguments: arguments.to_string(),
                });
            }
        }

        let usage = self.usage.map(|u| super::interface::TokenUsage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        });

        Ok(GenerateResponse {
            id,
            model,
            created: None,
            text: self.accumulated_text,
            usage,
            finish_reason: self.status,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            object: None,
            raw: None,
        })
    }
}

impl Default for EventAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// LanguageModel Trait Implementation
// ============================================================================

fn validate_request(request: &GenerateRequest) -> SdkResult<()> {
    if request.system_prompt.is_none() && request.messages.is_empty() {
        return Err(SdkError::Configuration(
            "at least a system prompt or one message is required for OpenAI Responses requests"
                .to_string(),
        ));
    }
    Ok(())
}

async fn ensure_success(response: reqwest::Response) -> SdkResult<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "<unable to read body>".to_string());

    if let Some(message) = parse_error(&body) {
        return Err(SdkError::Other(anyhow!(
            "OpenAI Responses API error ({status}): {message}"
        )));
    }

    Err(SdkError::Other(anyhow!(
        "OpenAI Responses API error ({status}): {body}"
    )))
}

#[async_trait]
impl LanguageModel for OpenAiProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        // Create OpenTelemetry span for this LLM call
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
        let result = async {
            validate_request(&request)?;
            let model = self.normalize_model(&request.model)?;
            let payload = ResponsesApiRequest::from_request(&request, model);

            let response = self
                .request()
                .json(&payload)
                .send()
                .await
                .map_err(|err| SdkError::Other(anyhow!("OpenAI Responses request failed: {err}")))?;

            let response = ensure_success(response).await?;

            let parsed: ResponsesApiResponse = response
                .json()
                .await
                .map_err(|err| {
                    SdkError::Other(anyhow!("failed to parse OpenAI Responses response: {err}"))
                })?;

            parsed.into_generate_response()
        }
        .await;

        // Record latency on the span
        let duration_ms = start.elapsed().as_millis();
        telemetry::set_duration(&mut span, duration_ms);

        // Handle result and set span attributes
        match result {
            Ok(response) => {
                telemetry::set_response_attributes(&mut span, &response, capture_content);
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

        // Execute the actual streaming API call
        let result = async {
            validate_request(&request)?;
            let model = self.normalize_model(&request.model)?;
            let mut payload = ResponsesApiRequest::from_request(&request, model);

            // Enable streaming
            payload.store = Some(false); // Stateless streaming

            let response = self
                .request()
                .header("Accept", "text/event-stream")
                .json(&payload)
                .send()
                .await
                .map_err(|err| {
                    SdkError::Other(anyhow!("OpenAI Responses streaming request failed: {err}"))
                })?;

            let response = ensure_success(response).await?;
            stream_handle_from_response(response, request.config.response_format.clone())
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
