use std::env;
use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{self, json, Value as JsonValue};

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    generate as generate_via_model, stream as stream_via_model, GenerateRequest, GenerateResponse,
    GenerationConfig, LanguageModel, Message, MessageRole, ResponseFormat, StreamChunk,
    StreamHandle, StreamRequest, TokenUsage, ToolChoice, ToolDefinition,
};

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
}

impl AnthropicConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            version: DEFAULT_VERSION.to_string(),
            timeout: DEFAULT_TIMEOUT,
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
        let api_key = env::var("ANTHROPIC_API_KEY")
            .map_err(|_| SdkError::Configuration("ANTHROPIC_API_KEY must be set".to_string()))?;

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
        let http = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|err| SdkError::Other(anyhow!("failed to construct HTTP client: {err}")))?;

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
        validate_request(&request)?;
        let payload = MessagesPayload::from_request(&request, false)?;
        let response = self
            .request()
            .json(&payload)
            .send()
            .await
            .map_err(|err| SdkError::Other(anyhow!("Anthropic request failed: {err}")))?;

        let response = ensure_success(response).await?;

        let parsed: MessagesResponse = response
            .json()
            .await
            .map_err(|err| SdkError::Other(anyhow!("failed to parse Anthropic response: {err}")))?;

        parsed.into_generate_response(request.config.response_format.clone())
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        validate_request(&request)?;
        let payload = MessagesPayload::from_request(&request, true)?;
        let response = self
            .request()
            .header("accept", "text/event-stream")
            .json(&payload)
            .send()
            .await
            .map_err(|err| SdkError::Other(anyhow!("Anthropic streaming request failed: {err}")))?;

        let response = ensure_success(response).await?;

        let stream = build_stream(response, request.config.response_format.clone());
        Ok(StreamHandle::new(stream))
    }
}

fn validate_request(request: &GenerateRequest) -> SdkResult<()> {
    if request.model.trim().is_empty() {
        return Err(SdkError::Configuration(
            "model must be provided for Anthropic requests".to_string(),
        ));
    }

    if request
        .messages
        .iter()
        .all(|message| message.role == MessageRole::System)
    {
        return Err(SdkError::Configuration(
            "at least one non-system message is required for Anthropic requests".to_string(),
        ));
    }

    if request.system_prompt.is_none() && request.messages.is_empty() {
        return Err(SdkError::Configuration(
            "at least a system prompt or one message is required for Anthropic requests"
                .to_string(),
        ));
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
                        .into_generate_response(aggregate.clone(), response_format.clone())?;
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
                    StreamEvent::ContentBlockStart { content_block, .. } => {
                        if let Some(delta) = content_block.to_text_delta() {
                            if !delta.is_empty() {
                                aggregate.push_str(&delta);
                                yield StreamChunk::Delta { content: delta };
                            }
                        } else if let Some(tool_json) = content_block.to_tool_use_json() {
                            yield StreamChunk::Delta { content: tool_json };
                        }
                    }
                    StreamEvent::ContentBlockDelta { delta, .. } => {
                        if let Some(text) = delta.text {
                            if !text.is_empty() {
                                aggregate.push_str(&text);
                                yield StreamChunk::Delta { content: text };
                            }
                        } else if let Some(input) = delta.input {
                            let tool_delta = json!({
                                "type": "tool_use_delta",
                                "input": input,
                            })
                            .to_string();
                            yield StreamChunk::Delta { content: tool_delta };
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
                            .into_generate_response(aggregate.clone(), response_format.clone())?;
                        yield StreamChunk::Completed(response);
                        return;
                    }
                    StreamEvent::ContentBlockStop { .. } => {}
                }
            }
        }

        Err(SdkError::Other(anyhow!("stream ended before termination signal")))?
    };

    Box::pin(stream)
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

    if let Ok(api_error) = serde_json::from_str::<ApiError>(&body) {
        return Err(SdkError::Other(anyhow!(
            "Anthropic API error ({status}): {}",
            api_error.error.message
        )));
    }

    Err(SdkError::Other(anyhow!(
        "Anthropic API error ({status}): {body}"
    )))
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<AnthropicTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<JsonValue>,
}

impl MessagesPayload {
    fn from_request(request: &GenerateRequest, stream: bool) -> SdkResult<Self> {
        let messages = request
            .messages
            .iter()
            .filter(|message| message.role != MessageRole::System)
            .map(AnthropicMessage::from)
            .collect::<Vec<_>>();
        let GenerationConfig {
            temperature,
            top_p,
            max_output_tokens,
            response_format,
        } = request.config.clone();

        let max_tokens = max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS).max(1);

        let model = normalize_model(&request.model)?;

        let system = augment_system_prompt(request.system_prompt.clone(), &response_format);
        let tools = convert_tools(&request.tools)?;
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

#[derive(Serialize)]
struct AnthropicMessage {
    role: String,
    content: Vec<TextBlock>,
}

impl From<&Message> for AnthropicMessage {
    fn from(message: &Message) -> Self {
        Self {
            role: message.role.as_str().to_string(),
            content: vec![TextBlock {
                content_type: "text".to_string(),
                text: Some(message.content.clone()),
            }],
        }
    }
}

#[derive(Serialize)]
struct TextBlock {
    #[serde(rename = "type")]
    content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
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

        for block in content {
            match block.block_type.as_str() {
                "text" => {
                    if let Some(text) = &block.text {
                        text_parts.push(text.clone());
                    }
                }
                "tool_use" => {}
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
            object,
            raw: Some(raw),
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
        Some(ToolChoice::Tool { name }) => Some(json!({"type": "tool", "name": name})),
    }
}

fn normalize_model(model: &str) -> SdkResult<String> {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return Err(SdkError::Configuration(
            "model id must not be empty for Anthropic requests".to_string(),
        ));
    }

    if let Some((provider, rest)) = trimmed.split_once('/') {
        let rest = rest.trim();
        if provider != "anthropic" {
            return Err(SdkError::Configuration(format!(
                "Anthropic provider expects model ids prefixed with `anthropic/`; got `{provider}`"
            )));
        }
        if rest.is_empty() {
            return Err(SdkError::Configuration(
                "model id must be provided after `anthropic/` prefix".to_string(),
            ));
        }
        Ok(rest.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

#[derive(Deserialize)]
struct ApiError {
    error: ApiErrorDetail,
}

#[derive(Deserialize)]
struct ApiErrorDetail {
    message: String,
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
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<JsonValue>,
}

impl StreamContentBlock {
    fn to_text_delta(&self) -> Option<String> {
        if self.block_type == "text" {
            self.text.clone()
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
            object,
            raw: None,
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
