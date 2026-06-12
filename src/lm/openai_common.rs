use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};

use anyhow::anyhow;
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use reqwest::Response;
use serde::{Deserialize, Serialize};
use serde_json::{self, json, Value as JsonValue};

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    ContentBlockType, GenerateRequest, GenerateResponse, GenerationConfig, JsonSchemaFormat,
    Message, MessageRole, ResponseFormat, StreamChunk, StreamHandle, TokenUsage, ToolCall,
    ToolChoice, ToolDefinition,
};

/// OpenAI reasoning models (gpt-5, o1, o3, o4 series) don't support sampling
/// parameters (`temperature`, `top_p`) and require `max_completion_tokens`
/// instead of `max_tokens`. Note: gpt-4o DOES support temperature.
pub(crate) fn is_reasoning_model(model: &str) -> bool {
    model.starts_with("gpt-5")
        || model == "o1"
        || model.starts_with("o1-")
        || model == "o3"
        || model.starts_with("o3-")
        || model == "o4"
        || model.starts_with("o4-")
}

/// Warn when configured sampling parameters are unsupported by the target
/// model and dropped from the request payload, so misconfiguration is never
/// silent. Deduplicated per (model, parameter) per process to avoid flooding
/// logs on every request.
pub(crate) fn warn_dropped_sampling_params(model: &str, config: &GenerationConfig) {
    static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

    for (param, value) in [("temperature", config.temperature), ("top_p", config.top_p)] {
        if value.is_none() {
            continue;
        }
        let mut warned = WARNED
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap();
        if warned.insert(format!("{model}/{param}")) {
            tracing::warn!(
                "⚠️  '{param}' is not supported by model '{model}' and will be ignored. \
                 Remove '{param}' from your configuration to silence this warning."
            );
        }
    }
}

#[derive(Serialize)]
pub(crate) struct ChatCompletionPayload {
    pub(crate) model: String,
    pub(crate) messages: Vec<ApiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_format: Option<ApiResponseFormat>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) tools: Vec<ApiTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tool_choice: Option<JsonValue>,
}

impl ChatCompletionPayload {
    pub(crate) fn from_request(request: &GenerateRequest, model: String, stream: bool) -> Self {
        let messages = build_api_messages(request);

        let is_reasoning_model = is_reasoning_model(&model);
        if is_reasoning_model {
            warn_dropped_sampling_params(&model, &request.config);
        }

        // Use max_completion_tokens for reasoning models, max_tokens for others
        let (max_tokens, max_completion_tokens) = if is_reasoning_model {
            (None, request.config.max_output_tokens)
        } else {
            (request.config.max_output_tokens, None)
        };

        // Exclude temperature and top_p for reasoning models
        let temperature = if is_reasoning_model {
            None
        } else {
            request.config.temperature
        };
        let top_p = if is_reasoning_model {
            None
        } else {
            request.config.top_p
        };

        let mut payload = Self {
            model,
            messages,
            temperature,
            top_p,
            max_tokens,
            max_completion_tokens,
            stream: stream.then_some(true),
            user: request.user_id.clone(),
            response_format: match &request.config.response_format {
                ResponseFormat::Text => None,
                ResponseFormat::Json => Some(ApiResponseFormat::json_object()),
                ResponseFormat::JsonSchema(schema) => Some(ApiResponseFormat::json_schema(schema)),
            },
            tools: api_tools_from_request(&request.tools),
            tool_choice: api_tool_choice_from_request(request.tool_choice.as_ref()),
        };

        if payload.max_tokens == Some(0) {
            payload.max_tokens = None;
        }
        if payload.max_completion_tokens == Some(0) {
            payload.max_completion_tokens = None;
        }

        payload
    }
}

#[derive(Serialize)]
pub(crate) struct ApiResponseFormat {
    #[serde(rename = "type")]
    r#type: ApiResponseFormatType,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_schema: Option<ApiJsonSchema>,
}

impl ApiResponseFormat {
    fn json_object() -> Self {
        Self {
            r#type: ApiResponseFormatType::JsonObject,
            json_schema: None,
        }
    }

    fn json_schema(schema: &JsonSchemaFormat) -> Self {
        Self {
            r#type: ApiResponseFormatType::JsonSchema,
            json_schema: Some(ApiJsonSchema {
                name: schema.name.clone(),
                schema: schema.schema.clone(),
                strict: schema.strict,
            }),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ApiResponseFormatType {
    JsonObject,
    JsonSchema,
}

#[derive(Serialize)]
pub(crate) struct ApiJsonSchema {
    pub(crate) name: String,
    pub(crate) schema: JsonValue,
    pub(crate) strict: bool,
}

#[derive(Serialize)]
pub(crate) struct ApiTool {
    #[serde(rename = "type")]
    r#type: &'static str,
    function: ApiToolFunction,
}

#[derive(Serialize)]
pub(crate) struct ApiToolFunction {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strict: Option<bool>,
}

/// API Message for Chat Completions API
/// Supports regular messages, assistant messages with tool_calls, and tool result messages
#[derive(Serialize)]
pub(crate) struct ApiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ApiToolCall>>,
}

impl ApiMessage {
    fn from_sdk_message(message: &Message) -> Self {
        // Tool result message
        if let Some(tool_call_id) = &message.tool_call_id {
            return Self {
                role: "tool".to_string(),
                content: Some(message.content.clone()),
                tool_call_id: Some(tool_call_id.clone()),
                tool_calls: None,
            };
        }

        // Assistant message with tool calls
        if let Some(tool_calls) = &message.tool_calls {
            let api_tool_calls: Vec<ApiToolCall> = tool_calls
                .iter()
                .map(|tc| ApiToolCall {
                    id: tc.id.clone(),
                    tool_type: "function".to_string(),
                    function: ApiToolCallFunction {
                        name: tc.name.clone(),
                        arguments: tc.arguments.clone(),
                    },
                })
                .collect();

            return Self {
                role: "assistant".to_string(),
                content: if message.content.is_empty() {
                    None
                } else {
                    Some(message.content.clone())
                },
                tool_call_id: None,
                tool_calls: Some(api_tool_calls),
            };
        }

        // Regular message
        Self {
            role: message.role.as_str().to_string(),
            content: Some(message.content.clone()),
            tool_call_id: None,
            tool_calls: None,
        }
    }
}

pub(crate) fn build_api_messages(request: &GenerateRequest) -> Vec<ApiMessage> {
    let mut messages = Vec::new();

    if let Some(system_prompt) = &request.system_prompt {
        messages.push(ApiMessage {
            role: MessageRole::System.as_str().to_string(),
            content: Some(system_prompt.clone()),
            tool_call_id: None,
            tool_calls: None,
        });
    }

    messages.extend(request.messages.iter().map(ApiMessage::from_sdk_message));
    messages
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ChatCompletionResponse {
    pub(crate) id: String,
    pub(crate) model: String,
    pub(crate) created: Option<u64>,
    pub(crate) choices: Vec<ChatCompletionChoice>,
    pub(crate) usage: Option<ApiUsage>,
}

impl ChatCompletionResponse {
    pub(crate) fn into_generate_response(
        self,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        use super::interface::ToolCall;

        let raw = serde_json::to_value(&self).unwrap_or(JsonValue::Null);
        let ChatCompletionResponse {
            id,
            model,
            created,
            choices,
            usage,
        } = self;

        let text = choices
            .first()
            .and_then(|choice| choice.message.as_ref())
            .and_then(|message| message.content.clone())
            .unwrap_or_default();

        let finish_reason = choices
            .first()
            .and_then(|choice| choice.finish_reason.clone());

        // Extract tool_calls from the response
        let tool_calls = choices
            .first()
            .and_then(|choice| choice.message.as_ref())
            .and_then(|message| message.tool_calls.as_ref())
            .map(|api_tool_calls| {
                api_tool_calls
                    .iter()
                    .map(|api_tc| ToolCall {
                        id: api_tc.id.clone(),
                        name: api_tc.function.name.clone(),
                        arguments: api_tc.function.arguments.clone(),
                    })
                    .collect()
            });

        let object = match response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json => Some(parse_json_value(&text)?),
            ResponseFormat::JsonSchema(_) => Some(parse_json_value(&text)?),
        };

        Ok(GenerateResponse {
            id,
            model,
            created,
            text,
            usage: usage_from_api(usage),
            finish_reason,
            tool_calls,
            object,
            raw: Some(raw),
            metadata: None,
        })
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ChatCompletionChoice {
    #[allow(unused)]
    index: Option<u32>,
    message: Option<ChoiceMessage>,
    finish_reason: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ChoiceMessage {
    #[allow(unused)]
    role: Option<String>,
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ApiToolCall>>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct ApiToolCall {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) tool_type: String,
    pub(crate) function: ApiToolCallFunction,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct ApiToolCallFunction {
    pub(crate) name: String,
    pub(crate) arguments: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct ApiUsage {
    pub(crate) prompt_tokens: Option<u32>,
    pub(crate) completion_tokens: Option<u32>,
    pub(crate) total_tokens: Option<u32>,
}

fn usage_from_api(usage: Option<ApiUsage>) -> Option<TokenUsage> {
    usage.map(|usage| TokenUsage {
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens,
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

fn api_tools_from_request(tools: &[ToolDefinition]) -> Vec<ApiTool> {
    tools
        .iter()
        .map(|tool| ApiTool {
            r#type: "function",
            function: ApiToolFunction {
                name: tool.name.clone(),
                description: tool.description.clone(),
                parameters: tool.parameters.clone(),
                strict: tool.strict,
            },
        })
        .collect()
}

fn api_tool_choice_from_request(choice: Option<&ToolChoice>) -> Option<JsonValue> {
    match choice {
        None => None,
        Some(ToolChoice::Auto) => Some(JsonValue::String("auto".to_string())),
        Some(ToolChoice::None) => Some(JsonValue::String("none".to_string())),
        Some(ToolChoice::Required) => Some(JsonValue::String("required".to_string())),
        Some(ToolChoice::Tool { name }) => Some(json!({
            "type": "function",
            "function": {
                "name": name,
            }
        })),
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ChatCompletionChunk {
    pub(crate) id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) created: Option<u64>,
    pub(crate) choices: Vec<ChunkChoice>,
    pub(crate) usage: Option<ApiUsage>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ChunkChoice {
    #[allow(unused)]
    index: Option<u32>,
    delta: ChunkDelta,
    finish_reason: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ChunkDelta {
    #[allow(unused)]
    role: Option<String>,
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<ApiToolCallDelta>>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ApiToolCallDelta {
    index: usize,
    id: Option<String>,
    #[serde(rename = "type")]
    #[allow(unused)]
    tool_type: Option<String>,
    function: Option<ApiToolCallFunctionDelta>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ApiToolCallFunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Default, Clone)]
struct PartialToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

#[derive(Default, Clone)]
struct PartialResponse {
    id: Option<String>,
    model: Option<String>,
    created: Option<u64>,
    finish_reason: Option<String>,
    usage: Option<ApiUsage>,
    tool_calls: Vec<PartialToolCall>,
}

impl PartialResponse {
    fn update(&mut self, chunk: &ChatCompletionChunk) {
        if let Some(id) = &chunk.id {
            self.id = Some(id.clone());
        }
        if let Some(model) = &chunk.model {
            self.model = Some(model.clone());
        }
        if let Some(created) = chunk.created {
            self.created = Some(created);
        }
        if let Some(usage) = &chunk.usage {
            self.usage = Some(usage.clone());
        }
    }

    fn update_tool_calls(&mut self, deltas: Vec<ApiToolCallDelta>) {
        for delta in deltas {
            while self.tool_calls.len() <= delta.index {
                self.tool_calls.push(PartialToolCall::default());
            }

            let partial = &mut self.tool_calls[delta.index];
            if let Some(id) = delta.id {
                partial.id = Some(id);
            }
            if let Some(function) = delta.function {
                if let Some(name) = function.name {
                    partial.name = Some(name);
                }
                if let Some(arguments) = function.arguments {
                    partial.arguments.push_str(&arguments);
                }
            }
        }
    }

    fn tool_calls(&self) -> Option<Vec<ToolCall>> {
        let tool_calls = self
            .tool_calls
            .iter()
            .enumerate()
            .filter_map(|(index, partial)| {
                partial.name.as_ref().map(|name| ToolCall {
                    id: partial
                        .id
                        .clone()
                        .unwrap_or_else(|| format!("call_{index}")),
                    name: name.clone(),
                    arguments: partial.arguments.clone(),
                })
            })
            .collect::<Vec<_>>();

        if tool_calls.is_empty() {
            None
        } else {
            Some(tool_calls)
        }
    }

    fn into_generate_response(
        self,
        text: String,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let tool_calls = self.tool_calls();
        let object = match response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json => Some(parse_json_value(&text)?),
            ResponseFormat::JsonSchema(_) => Some(parse_json_value(&text)?),
        };

        Ok(GenerateResponse {
            id: self.id.unwrap_or_default(),
            model: self.model.unwrap_or_default(),
            created: self.created,
            finish_reason: self.finish_reason,
            usage: usage_from_api(self.usage),
            text,
            tool_calls,
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

/// Format reqwest errors with clear, actionable messages for OpenAI-compatible APIs
fn format_streaming_error(err: &reqwest::Error, timeout_secs: u64) -> SdkError {
    if err.is_timeout() {
        SdkError::Other(anyhow!(
            "OpenAI API streaming timed out after {} seconds. \
            To increase, set OPENAI_REQUEST_TIMEOUT_SECS (e.g., OPENAI_REQUEST_TIMEOUT_SECS=1200)",
            timeout_secs
        ))
    } else if err.is_connect() {
        SdkError::Other(anyhow!(
            "OpenAI API streaming failed: Unable to connect. Check your network connection. Error: {}",
            err
        ))
    } else if err.is_decode() {
        SdkError::Other(anyhow!(
            "OpenAI API streaming failed: Unable to decode response. The response may be malformed. Error: {}",
            err
        ))
    } else {
        SdkError::Other(anyhow!("OpenAI API streaming failed: {}", err))
    }
}

pub(crate) fn stream_handle_from_response(
    response: Response,
    response_format: ResponseFormat,
    timeout_secs: u64,
) -> SdkResult<StreamHandle> {
    let stream = build_stream(response, response_format, timeout_secs)?;
    Ok(StreamHandle::new(stream))
}

fn build_stream(
    response: Response,
    response_format: ResponseFormat,
    timeout_secs: u64,
) -> SdkResult<Pin<Box<dyn Stream<Item = SdkResult<StreamChunk>> + Send>>> {
    let bytes_stream = response.bytes_stream();

    let stream = try_stream! {
        futures::pin_mut!(bytes_stream);
        let mut decoder = SseDecoder::default();
        let mut aggregate = String::new();
        let mut partial = PartialResponse::default();
        let mut content_block_started = false;

        while let Some(chunk) = bytes_stream.next().await {
            let chunk = chunk.map_err(|err| format_streaming_error(&err, timeout_secs))?;
            for event in decoder.ingest(chunk.as_ref())? {
                let data = event.trim();
                if data.is_empty() {
                    continue;
                }

                if data == "[DONE]" {
                    // Close any open content block
                    if content_block_started {
                        yield StreamChunk::ContentBlockStop { index: 0 };
                    }
                    let response = partial.into_generate_response(aggregate.clone(), response_format)?;
                    yield StreamChunk::Completed(response);
                    return;
                }

                let parsed: ChatCompletionChunk = serde_json::from_str(data)
                    .map_err(|err| SdkError::Other(anyhow!("failed to parse OpenAI-style stream chunk: {err}")))?;

                partial.update(&parsed);

                for choice in parsed.choices {
                    if let Some(tool_calls) = choice.delta.tool_calls {
                        partial.update_tool_calls(tool_calls);
                    }

                    if let Some(content) = choice.delta.content {
                        if !content.is_empty() {
                            // Emit ContentBlockStart on first content
                            if !content_block_started {
                                yield StreamChunk::ContentBlockStart {
                                    index: 0,
                                    block_type: ContentBlockType::Text,
                                };
                                content_block_started = true;
                            }
                            aggregate.push_str(&content);
                            yield StreamChunk::Delta {
                                content,
                                index: 0,
                                block_type: ContentBlockType::Text,
                            };
                        }
                    }

                    if let Some(reason) = choice.finish_reason {
                        if partial.finish_reason.is_none() {
                            partial.finish_reason = Some(reason);
                        }
                    }
                }
            }
        }

        Err(SdkError::Other(anyhow!("stream ended before termination signal")))?
    };

    Ok(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_reasoning_models() {
        for model in [
            "gpt-5",
            "gpt-5-mini",
            "gpt-5-nano",
            "o1",
            "o1-preview",
            "o3",
            "o3-mini",
            "o4",
            "o4-mini",
        ] {
            assert!(is_reasoning_model(model), "{model} should be reasoning");
        }
        for model in ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-3.5-turbo"] {
            assert!(
                !is_reasoning_model(model),
                "{model} should not be reasoning"
            );
        }
    }

    #[test]
    fn chat_payload_drops_sampling_params_for_reasoning_models() {
        let request = GenerateRequest::new("openai/gpt-5-mini")
            .user_message("Hi")
            .configure(|c| {
                c.temperature = Some(0.7);
                c.top_p = Some(0.9);
            });

        let payload =
            ChatCompletionPayload::from_request(&request, "gpt-5-mini".to_string(), false);
        assert!(payload.temperature.is_none());
        assert!(payload.top_p.is_none());
    }

    #[test]
    fn partial_response_accumulates_streaming_tool_call_deltas() {
        let mut partial = PartialResponse::default();

        partial.update_tool_calls(vec![ApiToolCallDelta {
            index: 0,
            id: Some("call_123".to_string()),
            tool_type: Some("function".to_string()),
            function: Some(ApiToolCallFunctionDelta {
                name: Some("lookup_weather".to_string()),
                arguments: Some("{\"city\"".to_string()),
            }),
        }]);
        partial.update_tool_calls(vec![ApiToolCallDelta {
            index: 0,
            id: None,
            tool_type: None,
            function: Some(ApiToolCallFunctionDelta {
                name: None,
                arguments: Some(":\"SF\"}".to_string()),
            }),
        }]);

        let response = partial
            .into_generate_response(String::new(), ResponseFormat::Text)
            .unwrap();
        let tool_calls = response.tool_calls.unwrap();

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "call_123");
        assert_eq!(tool_calls[0].name, "lookup_weather");
        assert_eq!(tool_calls[0].arguments, "{\"city\":\"SF\"}");
    }
}
