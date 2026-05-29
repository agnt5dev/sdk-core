use std::env;
use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use opentelemetry::trace::Span;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{self, json, Value as JsonValue};

use crate::error::{Result as SdkResult, SdkError};

use super::http;
use super::interface::{
    generate as generate_via_model, stream as stream_via_model, BuiltInTool, ContentBlockType,
    GenerateRequest, GenerateResponse, GenerationConfig, LanguageModel, Message, MessageRole,
    ResponseFormat, StreamChunk, StreamHandle, StreamRequest, TokenUsage, ToolChoice,
    ToolDefinition,
};
use super::telemetry;

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const DEFAULT_VERSION: &str = "2023-06-01";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Configuration for the Anthropic provider.
#[derive(Clone, Debug)]
pub struct AnthropicConfig {
    pub api_key: String,
    pub base_url: String,
    pub version: String,
    pub timeout: Duration,
    pub retry_config: http::RetryConfig,
}

impl AnthropicConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            version: DEFAULT_VERSION.to_string(),
            timeout: DEFAULT_TIMEOUT,
            retry_config: http::RetryConfig::from_env(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn from_env() -> SdkResult<Self> {
        let api_key = env::var("ANTHROPIC_API_KEY").map_err(|_| SdkError::Configuration {
            message: "ANTHROPIC_API_KEY must be set".to_string(),
            field: Some("ANTHROPIC_API_KEY".to_string()),
        })?;

        let mut config = AnthropicConfig::new(api_key);

        if let Ok(base_url) = env::var("ANTHROPIC_BASE_URL") {
            if !base_url.trim().is_empty() {
                config.base_url = base_url;
            }
        }

        if let Ok(version) = env::var("ANTHROPIC_API_VERSION") {
            if !version.trim().is_empty() {
                config.version = version;
            }
        }

        if let Ok(timeout) = env::var("ANTHROPIC_TIMEOUT_SECS") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.timeout = Duration::from_secs(secs);
            }
        }

        Ok(config)
    }
}

/// Minimal provider implementation for Anthropic models.
#[derive(Clone)]
pub struct AnthropicProvider {
    http: Client,
    config: AnthropicConfig,
}

impl AnthropicProvider {
    pub fn new(config: AnthropicConfig) -> SdkResult<Self> {
        let http = http::build_http_client(config.timeout)?;

        Ok(Self { http, config })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = AnthropicConfig::from_env()?;
        Self::new(config)
    }

    fn messages_endpoint(&self) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        format!("{base}/v1/messages")
    }

    fn request(&self) -> reqwest::RequestBuilder {
        self.http
            .post(self.messages_endpoint())
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", &self.config.version)
            .header("content-type", "application/json")
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        generate_via_model(self, request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        stream_via_model(self, request).await
    }
}

#[async_trait]
impl LanguageModel for AnthropicProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        // Create OpenTelemetry span for this LLM call as child of the current execution span
        let mut span = telemetry::create_gen_ai_span(
            "anthropic",
            &request.model,
            request.otel_context.clone(),
        );

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

        let start = std::time::Instant::now();

        let result: SdkResult<GenerateResponse> = async {
            validate_request(&request)?;
            let payload = MessagesPayload::from_request(&request, false)?;
            let response = http::send_with_retry(
                || self.request().json(&payload),
                &self.config.retry_config,
                "anthropic",
                request.config.timeout,
            )
            .await?;

            let metadata = http::extract_metadata(&response);
            let parsed: MessagesResponse = response.json().await.map_err(|err| {
                SdkError::Other(anyhow!("failed to parse Anthropic response: {err}"))
            })?;

            let mut result =
                parsed.into_generate_response(request.config.response_format.clone())?;
            result.metadata = Some(metadata);
            Ok(result)
        }
        .await;

        let duration_ms = start.elapsed().as_millis();
        telemetry::set_duration(&mut span, duration_ms);

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
                            "anthropic",
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
        // Create OpenTelemetry span for streaming LLM call
        let mut span = telemetry::create_gen_ai_span(
            "anthropic",
            &request.model,
            request.otel_context.clone(),
        );

        telemetry::set_request_attributes(&mut span, &request);
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

        let start = std::time::Instant::now();

        let result: SdkResult<StreamHandle> = async {
            validate_request(&request)?;
            let payload = MessagesPayload::from_request(&request, true)?;
            let response = http::send_with_retry(
                || {
                    self.request()
                        .header("accept", "text/event-stream")
                        .json(&payload)
                },
                &self.config.retry_config,
                "anthropic",
                request.config.timeout,
            )
            .await?;

            let stream = build_stream(response, request.config.response_format.clone());
            Ok(StreamHandle::new(stream))
        }
        .await;

        let duration_ms = start.elapsed().as_millis();
        telemetry::set_duration(&mut span, duration_ms);

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

fn validate_request(request: &GenerateRequest) -> SdkResult<()> {
    if request.model.trim().is_empty() {
        return Err(SdkError::Configuration {
            message: "model must be provided for Anthropic requests".to_string(),
            field: Some("model".to_string()),
        });
    }

    if request
        .messages
        .iter()
        .all(|message| message.role == MessageRole::System)
    {
        return Err(SdkError::Configuration {
            message: "at least one non-system message is required for Anthropic requests"
                .to_string(),
            field: None,
        });
    }

    if request.system_prompt.is_none() && request.messages.is_empty() {
        return Err(SdkError::Configuration {
            message: "at least a system prompt or one message is required for Anthropic requests"
                .to_string(),
            field: None,
        });
    }

    Ok(())
}

fn build_stream(
    response: reqwest::Response,
    response_format: ResponseFormat,
) -> Pin<Box<dyn futures::Stream<Item = SdkResult<StreamChunk>> + Send>> {
    let bytes_stream = response.bytes_stream();

    let stream = try_stream! {
        futures::pin_mut!(bytes_stream);
        let mut decoder = SseDecoder::default();
        let mut aggregate = String::new();
        let mut partial = PartialResponse::default();
        let mut tool_calls: Vec<super::interface::ToolCall> = Vec::new();
        // Track current content block for proper typing of deltas
        let mut _current_block_index: u32 = 0;
        let mut current_block_type = ContentBlockType::Text;

        while let Some(chunk) = bytes_stream.next().await {
            let chunk = chunk.map_err(|err| SdkError::Other(anyhow!("error reading streaming chunk: {err}")))?;
            for event in decoder.ingest(chunk.as_ref())? {
                let trimmed = event.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if trimmed == "[DONE]" {
                    let response = partial
                        .clone()
                        .into_generate_response(aggregate.clone(), tool_calls.clone(), response_format.clone())?;
                    yield StreamChunk::Completed(response);
                    return;
                }

                let parsed: StreamEvent = serde_json::from_str(trimmed)
                    .map_err(|err| SdkError::Other(anyhow!("failed to parse Anthropic stream event: {err}")))?;

                match parsed {
                    StreamEvent::MessageStart { message } => {
                        partial.id = Some(message.id);
                        partial.model = Some(message.model);
                        partial.usage = Some(message.usage);
                    }
                    StreamEvent::ContentBlockStart { index, content_block } => {
                        _current_block_index = index;
                        current_block_type = content_block.content_block_type();

                        // Emit content block start
                        yield StreamChunk::ContentBlockStart {
                            index,
                            block_type: current_block_type,
                        };

                        // Extract tool_use blocks and accumulate tool calls
                        if content_block.block_type == "tool_use" {
                            if let (Some(id), Some(name)) = (&content_block.id, &content_block.name) {
                                let input = content_block.input.clone().unwrap_or_else(|| json!({}));
                                tool_calls.push(super::interface::ToolCall {
                                    id: id.clone(),
                                    name: name.clone(),
                                    arguments: input.to_string(),
                                });
                            }
                        }

                        // Handle initial content (if any)
                        if let Some(initial) = content_block.initial_content() {
                            if !initial.is_empty() {
                                // Only aggregate text content (not thinking)
                                if current_block_type == ContentBlockType::Text {
                                    aggregate.push_str(&initial);
                                }
                                yield StreamChunk::Delta {
                                    content: initial,
                                    index,
                                    block_type: current_block_type,
                                };
                            }
                        } else if let Some(tool_json) = content_block.to_tool_use_json() {
                            yield StreamChunk::Delta {
                                content: tool_json,
                                index,
                                block_type: ContentBlockType::Text, // Tool calls are text blocks
                            };
                        }
                    }
                    StreamEvent::ContentBlockDelta { index, delta } => {
                        if let Some(text) = delta.text {
                            if !text.is_empty() {
                                // Only aggregate text content (not thinking)
                                if current_block_type == ContentBlockType::Text {
                                    aggregate.push_str(&text);
                                }
                                yield StreamChunk::Delta {
                                    content: text,
                                    index,
                                    block_type: current_block_type,
                                };
                            }
                        } else if let Some(thinking) = delta.thinking {
                            // Handle thinking content delta
                            if !thinking.is_empty() {
                                yield StreamChunk::Delta {
                                    content: thinking,
                                    index,
                                    block_type: ContentBlockType::Thinking,
                                };
                            }
                        } else if let Some(input) = delta.input {
                            let tool_delta = json!({
                                "type": "tool_use_delta",
                                "input": input,
                            })
                            .to_string();
                            yield StreamChunk::Delta {
                                content: tool_delta,
                                index,
                                block_type: ContentBlockType::Text,
                            };
                        }
                    }
                    StreamEvent::MessageDelta { delta, usage } => {
                        if let Some(reason) = delta.stop_reason {
                            partial.stop_reason = Some(reason);
                        }
                        if let Some(usage) = usage {
                            partial.usage = Some(usage);
                        }
                    }
                    StreamEvent::MessageStop => {
                        let response = partial
                            .clone()
                            .into_generate_response(aggregate.clone(), tool_calls.clone(), response_format.clone())?;
                        yield StreamChunk::Completed(response);
                        return;
                    }
                    StreamEvent::ContentBlockStop { index } => {
                        yield StreamChunk::ContentBlockStop { index };
                    }
                    StreamEvent::Unknown => {
                        // Silently ignore unknown events (ping, error, etc.)
                    }
                }
            }
        }

        Err(SdkError::Other(anyhow!("stream ended before termination signal")))?
    };

    Box::pin(stream)
}

#[derive(Serialize)]
struct MessagesPayload {
    model: String,
    messages: Vec<AnthropicMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
    // Mixed list: user-defined tools and provider-hosted built-ins (e.g. web_search_20250305).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<JsonValue>,
}

impl MessagesPayload {
    fn from_request(request: &GenerateRequest, stream: bool) -> SdkResult<Self> {
        let messages = request
            .messages
            .iter()
            .filter(|message| message.role != MessageRole::System)
            .map(AnthropicMessage::from_sdk_message)
            .collect::<Vec<_>>();
        let GenerationConfig {
            temperature,
            top_p,
            max_output_tokens,
            response_format,
            reasoning_effort: _,
            modalities: _,
            built_in_tools,
            timeout: _,
        } = request.config.clone();

        let max_tokens = max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS).max(1);

        let model = normalize_model(&request.model)?;

        let system = augment_system_prompt(request.system_prompt.clone(), &response_format);

        // Build a mixed tools array: user-defined function tools + Anthropic
        // server-side built-ins (web_search_20250305 today). The Agent loop
        // recognizes built-in names and skips local dispatch.
        let mut tools: Vec<JsonValue> = convert_tools(&request.tools)?
            .into_iter()
            .map(|t| serde_json::to_value(t).unwrap_or(JsonValue::Null))
            .collect();

        for built_in in &built_in_tools {
            if let Some(spec) = anthropic_built_in_spec(built_in) {
                tools.push(spec);
            }
        }

        let tool_choice = convert_tool_choice(request.tool_choice.as_ref());

        Ok(Self {
            model,
            messages,
            system,
            max_tokens,
            temperature,
            top_p,
            stream: stream.then_some(true),
            tools,
            tool_choice,
        })
    }
}

/// Map a generic BuiltInTool to its Anthropic Messages-API tool spec, if any.
/// Returns None for variants Anthropic does not host (CodeInterpreter,
/// FileSearch are OpenAI-only today).
///
/// Versioning note: this defaults to the `web_search_20260209` /
/// `web_fetch_20260209` line, which supports dynamic filtering. Those tool
/// versions are model-gated to Mythos Preview / Opus 4.7 / Opus 4.6 / Sonnet
/// 4.6. Older Anthropic models will reject these and need the older
/// `web_search_20250305` / `web_fetch_20250910` versions; expose this as a
/// per-tool config knob if older-model support becomes a requirement.
fn anthropic_built_in_spec(tool: &BuiltInTool) -> Option<JsonValue> {
    match tool {
        BuiltInTool::WebSearch => Some(json!({
            "type": "web_search_20260209",
            "name": "web_search",
        })),
        BuiltInTool::WebFetch => Some(json!({
            "type": "web_fetch_20260209",
            "name": "web_fetch",
        })),
        BuiltInTool::CodeInterpreter | BuiltInTool::FileSearch => None,
    }
}

#[derive(Serialize)]
struct AnthropicMessage {
    role: String,
    content: Vec<ContentBlock>,
}

impl AnthropicMessage {
    fn from_sdk_message(message: &Message) -> Self {
        let mut content = Vec::new();

        // Tool result message
        if let Some(tool_call_id) = &message.tool_call_id {
            content.push(ContentBlock::ToolResult {
                tool_use_id: tool_call_id.clone(),
                content: message.content.clone(),
            });
            return Self {
                role: "user".to_string(), // Tool results are always user role
                content,
            };
        }

        // Assistant message with tool calls
        if let Some(tool_calls) = &message.tool_calls {
            // Add text content if present
            if !message.content.is_empty() {
                content.push(ContentBlock::Text {
                    text: message.content.clone(),
                });
            }

            // Add tool_use blocks for each tool call
            for tc in tool_calls {
                let input: JsonValue =
                    serde_json::from_str(&tc.arguments).unwrap_or_else(|_| json!({}));
                content.push(ContentBlock::ToolUse {
                    id: tc.id.clone(),
                    name: tc.name.clone(),
                    input,
                });
            }

            return Self {
                role: "assistant".to_string(),
                content,
            };
        }

        // Regular message
        content.push(ContentBlock::Text {
            text: message.content.clone(),
        });

        Self {
            role: message.role.as_str().to_string(),
            content,
        }
    }
}

/// Content block types for Anthropic Messages API
#[derive(Serialize)]
#[serde(tag = "type")]
enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: JsonValue,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

#[derive(Serialize)]
struct AnthropicTool {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    input_schema: JsonValue,
}

#[derive(Deserialize, Serialize)]
struct MessagesResponse {
    id: String,
    model: String,
    content: Vec<TextBlockResponse>,
    #[serde(rename = "stop_reason")]
    stop_reason: Option<String>,
    usage: Option<Usage>,
}

impl MessagesResponse {
    fn into_generate_response(
        self,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let raw = serde_json::to_value(&self).unwrap_or(JsonValue::Null);
        let MessagesResponse {
            id,
            model,
            content,
            stop_reason,
            usage,
        } = self;

        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();

        for block in content {
            match block.block_type.as_str() {
                "text" => {
                    if let Some(text) = &block.text {
                        text_parts.push(text.clone());
                    }
                }
                "tool_use" => {
                    // Extract tool call information from tool_use block
                    if let (Some(id), Some(name), Some(input)) =
                        (&block.id, &block.name, &block.input)
                    {
                        tool_calls.push(super::interface::ToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            arguments: input.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }

        let text = text_parts.join("");

        let object = match response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json => Some(parse_json_value(&text)?),
            ResponseFormat::JsonSchema(_) => Some(parse_json_value(&text)?),
        };

        Ok(GenerateResponse {
            id,
            model,
            created: None,
            text,
            usage: usage_from_api(usage),
            finish_reason: stop_reason,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            object,
            raw: Some(raw),
            metadata: None,
        })
    }
}

#[derive(Deserialize, Serialize)]
struct TextBlockResponse {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<JsonValue>,
}

#[derive(Deserialize, Serialize, Clone)]
struct Usage {
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
}

fn usage_from_api(usage: Option<Usage>) -> Option<TokenUsage> {
    usage.map(|usage| TokenUsage {
        prompt_tokens: usage.input_tokens,
        completion_tokens: usage.output_tokens,
        total_tokens: usage
            .input_tokens
            .and_then(|input| usage.output_tokens.map(|output| input + output)),
    })
}

fn parse_json_value(text: &str) -> SdkResult<JsonValue> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(SdkError::Other(anyhow!(
            "expected JSON response but model returned empty content"
        )));
    }

    serde_json::from_str(trimmed)
        .map_err(|err| SdkError::Other(anyhow!("failed to parse JSON response: {err}")))
}

fn augment_system_prompt(existing: Option<String>, format: &ResponseFormat) -> Option<String> {
    let mut system = existing.unwrap_or_default();
    if let Some(instruction) = response_format_instruction(format) {
        if !system.trim().is_empty() {
            system.push_str("\n\n");
        }
        system.push_str(&instruction);
    }

    if system.trim().is_empty() {
        None
    } else {
        Some(system)
    }
}

fn response_format_instruction(format: &ResponseFormat) -> Option<String> {
    match format {
        ResponseFormat::Text => None,
        ResponseFormat::Json => Some("Please respond with a valid JSON object.".to_string()),
        ResponseFormat::JsonSchema(schema) => {
            let schema_text = serde_json::to_string_pretty(&schema.schema)
                .unwrap_or_else(|_| schema.schema.to_string());
            Some(format!(
                "Respond with a JSON object matching the following schema (strict={}):\n{}",
                schema.strict, schema_text
            ))
        }
    }
}

fn convert_tools(tools: &[ToolDefinition]) -> SdkResult<Vec<AnthropicTool>> {
    let mut result = Vec::new();
    for tool in tools {
        let schema = tool.parameters.clone().unwrap_or_else(|| {
            json!({
                "type": "object",
                "properties": {},
            })
        });

        result.push(AnthropicTool {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: schema,
        });
    }
    Ok(result)
}

fn convert_tool_choice(choice: Option<&ToolChoice>) -> Option<JsonValue> {
    match choice {
        None => None,
        Some(ToolChoice::Auto) => Some(json!({"type": "auto"})),
        Some(ToolChoice::None) => Some(json!({"type": "none"})),
        Some(ToolChoice::Required) => Some(json!({"type": "any"})), // Anthropic uses "any" for required
        Some(ToolChoice::Tool { name }) => Some(json!({"type": "tool", "name": name})),
    }
}

fn normalize_model(model: &str) -> SdkResult<String> {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return Err(SdkError::Configuration {
            message: "model id must not be empty for Anthropic requests".to_string(),
            field: Some("model".to_string()),
        });
    }

    if let Some((provider, rest)) = trimmed.split_once('/') {
        let rest = rest.trim();
        if provider != "anthropic" {
            return Err(SdkError::Configuration {
                message: format!(
                    "Anthropic provider expects model ids prefixed with `anthropic/`; got `{provider}`"
                ),
                field: Some("model".to_string()),
            });
        }
        if rest.is_empty() {
            return Err(SdkError::Configuration {
                message: "model id must be provided after `anthropic/` prefix".to_string(),
                field: Some("model".to_string()),
            });
        }
        Ok(rest.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum StreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: StreamMessage },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        #[allow(unused)]
        index: u32,
        content_block: StreamContentBlock,
    },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta {
        #[allow(unused)]
        index: u32,
        delta: StreamContentDelta,
    },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop {
        #[allow(unused)]
        index: u32,
    },
    #[serde(rename = "message_delta")]
    MessageDelta {
        delta: StreamMessageDelta,
        usage: Option<Usage>,
    },
    #[serde(rename = "message_stop")]
    MessageStop,
    /// Catch-all for unknown events (e.g., "ping", "error")
    /// These are silently ignored to keep the stream alive.
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct StreamMessage {
    id: String,
    model: String,
    usage: Usage,
}

#[derive(Deserialize)]
struct StreamContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    text: Option<String>,
    /// Thinking content (for extended thinking models)
    #[serde(default)]
    thinking: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<JsonValue>,
}

impl StreamContentBlock {
    /// Get the content block type for this block.
    fn content_block_type(&self) -> ContentBlockType {
        match self.block_type.as_str() {
            "thinking" => ContentBlockType::Thinking,
            _ => ContentBlockType::Text,
        }
    }

    /// Check if this is a thinking block.
    fn is_thinking(&self) -> bool {
        self.block_type == "thinking"
    }

    /// Get the initial content for this block (if any).
    fn initial_content(&self) -> Option<String> {
        if self.is_thinking() {
            self.thinking.clone()
        } else {
            self.text.clone()
        }
    }

    #[allow(dead_code)]
    fn to_text_delta(&self) -> Option<String> {
        if self.block_type == "text" {
            self.text.clone()
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn to_thinking_delta(&self) -> Option<String> {
        if self.block_type == "thinking" {
            self.thinking.clone()
        } else {
            None
        }
    }

    fn to_tool_use_json(&self) -> Option<String> {
        if self.block_type == "tool_use" {
            let json = json!({
                "type": "tool_use",
                "id": self.id,
                "name": self.name,
                "input": self.input.clone().unwrap_or_else(|| json!({})),
            });
            Some(json.to_string())
        } else {
            None
        }
    }
}

#[derive(Deserialize)]
struct StreamContentDelta {
    #[serde(rename = "type")]
    #[allow(unused)]
    delta_type: String,
    text: Option<String>,
    /// Thinking content delta (for extended thinking models)
    #[serde(default)]
    thinking: Option<String>,
    #[serde(default)]
    input: Option<JsonValue>,
}

#[derive(Deserialize)]
struct StreamMessageDelta {
    stop_reason: Option<String>,
}

#[derive(Default, Clone)]
struct PartialResponse {
    id: Option<String>,
    model: Option<String>,
    usage: Option<Usage>,
    stop_reason: Option<String>,
}

impl PartialResponse {
    fn into_generate_response(
        self,
        text: String,
        tool_calls: Vec<super::interface::ToolCall>,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let usage = usage_from_api(self.usage.clone());
        let object = match response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json => Some(parse_json_value(&text)?),
            ResponseFormat::JsonSchema(_) => Some(parse_json_value(&text)?),
        };

        Ok(GenerateResponse {
            id: self.id.unwrap_or_default(),
            model: self.model.unwrap_or_default(),
            created: None,
            text,
            usage,
            finish_reason: self.stop_reason,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            object,
            raw: None,
            metadata: None,
        })
    }
}

#[derive(Default)]
struct SseDecoder {
    buffer: String,
}

impl SseDecoder {
    fn ingest(&mut self, chunk: &[u8]) -> SdkResult<Vec<String>> {
        let chunk_str = std::str::from_utf8(chunk)
            .map_err(|err| SdkError::Other(anyhow!("invalid UTF-8 in SSE stream: {err}")))?;
        self.buffer.push_str(chunk_str);

        let mut events = Vec::new();
        loop {
            if let Some(idx) = find_event_delimiter(&self.buffer) {
                let (event, remaining) = self.buffer.split_at(idx);
                let delimiter_len = delimiter_length(remaining);
                let event = event.to_string();
                self.buffer = remaining[delimiter_len..].to_string();

                let mut data = String::new();
                for line in event.lines() {
                    if let Some(rest) = line.strip_prefix("data:") {
                        if !data.is_empty() {
                            data.push('\n');
                        }
                        data.push_str(rest.trim_start());
                    }
                }

                if !data.is_empty() {
                    events.push(data);
                }
            } else {
                break;
            }
        }

        Ok(events)
    }
}

fn find_event_delimiter(buffer: &str) -> Option<usize> {
    buffer.find("\n\n").or_else(|| buffer.find("\r\n\r\n"))
}

fn delimiter_length(remaining: &str) -> usize {
    if remaining.starts_with("\r\n\r\n") {
        4
    } else {
        2
    }
}
