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

use super::interface::{
    generate as generate_via_model, stream as stream_via_model, ContentBlockType, GenerateRequest,
    GenerateResponse, GenerationConfig, LanguageModel, Message, MessageRole, ResponseFormat,
    StreamChunk, StreamHandle, StreamRequest, TokenUsage, ToolChoice, ToolDefinition,
};
use super::telemetry;

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com";
const DEFAULT_VERSION: &str = "v1beta";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_MAX_TOKENS: u32 = 2048;

/// Configuration for the Google Gemini provider.
///
/// Google's Gemini models offer strong multimodal capabilities,
/// long context windows, and excellent cost-efficiency with Flash models.
#[derive(Clone, Debug)]
pub struct GoogleConfig {
    pub api_key: String,
    pub base_url: String,
    pub version: String,
    pub timeout: Duration,
}

impl GoogleConfig {
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
        // Try GOOGLE_API_KEY first, then GEMINI_API_KEY for backwards compatibility
        let api_key = env::var("GOOGLE_API_KEY")
            .or_else(|_| env::var("GEMINI_API_KEY"))
            .map_err(|_| SdkError::Configuration {
                message: "GOOGLE_API_KEY or GEMINI_API_KEY must be set".to_string(),
                field: Some("GOOGLE_API_KEY".to_string()),
            })?;

        let mut config = GoogleConfig::new(api_key);

        if let Ok(base_url) = env::var("GOOGLE_BASE_URL") {
            if !base_url.trim().is_empty() {
                config.base_url = base_url;
            }
        }

        if let Ok(version) = env::var("GOOGLE_API_VERSION") {
            if !version.trim().is_empty() {
                config.version = version;
            }
        }

        if let Ok(timeout) = env::var("GOOGLE_TIMEOUT_SECS") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.timeout = Duration::from_secs(secs);
            }
        }

        Ok(config)
    }
}

/// Provider implementation for Google Gemini models.
///
/// Supports the full Gemini model family including:
/// - Gemini 2.0 Flash (fast, cost-effective)
/// - Gemini 1.5 Pro (high capability, long context)
/// - Gemini 1.5 Flash (balanced performance)
///
/// # Example
///
/// ```no_run
/// use agnt5_sdk_core::lm::{GoogleProvider, GenerateRequest};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = GoogleProvider::from_env()?;
/// let response = provider.generate(
///     GenerateRequest::new("google/gemini-2.0-flash")
///         .user_message("Explain neural networks")
/// ).await?;
/// println!("{}", response.text);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct GoogleProvider {
    http: Client,
    config: GoogleConfig,
}

impl GoogleProvider {
    pub fn new(config: GoogleConfig) -> SdkResult<Self> {
        let http = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|err| SdkError::Other(anyhow!("failed to construct HTTP client: {err}")))?;

        Ok(Self { http, config })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = GoogleConfig::from_env()?;
        Self::new(config)
    }

    fn generate_endpoint(&self, model: &str) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        format!(
            "{base}/{}/models/{}:generateContent?key={}",
            self.config.version, model, self.config.api_key
        )
    }

    fn stream_endpoint(&self, model: &str) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        format!(
            "{base}/{}/models/{}:streamGenerateContent?alt=sse&key={}",
            self.config.version, model, self.config.api_key
        )
    }

    fn request(&self, url: &str) -> reqwest::RequestBuilder {
        self.http
            .post(url)
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
impl LanguageModel for GoogleProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        let mut span =
            telemetry::create_gen_ai_span("google", &request.model, request.otel_context.clone());
        telemetry::set_request_attributes(&mut span, &request);

        let capture_content = telemetry::should_capture_content();
        telemetry::set_tool_request_attributes(&mut span, &request, capture_content);

        if capture_content {
            if let Some(system_prompt) = &request.system_prompt {
                let system_instructions = telemetry::serialize_system_instructions(system_prompt);
                span.set_attribute(opentelemetry::KeyValue::new(
                    telemetry::attributes::SYSTEM_INSTRUCTIONS,
                    system_instructions.to_string(),
                ));
            }

            let input_messages = telemetry::serialize_input_messages(&request);
            span.set_attribute(opentelemetry::KeyValue::new(
                telemetry::attributes::INPUT_MESSAGES,
                input_messages.to_string(),
            ));
        }

        let start = std::time::Instant::now();

        let result = async {
            validate_request(&request)?;
            let model = normalize_model(&request.model)?;
            let payload = GeminiPayload::from_request(&request)?;
            let url = self.generate_endpoint(&model);

            let response = self
                .request(&url)
                .json(&payload)
                .send()
                .await
                .map_err(|err| SdkError::Other(anyhow!("Google API request failed: {err}")))?;

            let response = ensure_success(response).await?;

            let parsed: GeminiResponse = response
                .json()
                .await
                .map_err(|err| SdkError::Other(anyhow!("failed to parse Google response: {err}")))?;

            parsed.into_generate_response(&model, request.config.response_format.clone())
        }
        .await;

        let duration_ms = start.elapsed().as_millis();
        telemetry::set_duration(&mut span, duration_ms);

        match result {
            Ok(response) => {
                telemetry::set_response_attributes(&mut span, &response, capture_content);

                if let Some(usage) = &response.usage {
                    if let (Some(input_tokens), Some(output_tokens)) =
                        (usage.prompt_tokens, usage.completion_tokens)
                    {
                        if let Some(cost) = telemetry::calculate_cost(
                            "google",
                            &response.model,
                            input_tokens,
                            output_tokens,
                            None,
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
        let mut span =
            telemetry::create_gen_ai_span("google", &request.model, request.otel_context.clone());
        telemetry::set_request_attributes(&mut span, &request);
        span.set_attribute(opentelemetry::KeyValue::new("llm.streaming", true));

        let capture_content = telemetry::should_capture_content();
        telemetry::set_tool_request_attributes(&mut span, &request, capture_content);

        if capture_content {
            if let Some(system_prompt) = &request.system_prompt {
                let system_instructions = telemetry::serialize_system_instructions(system_prompt);
                span.set_attribute(opentelemetry::KeyValue::new(
                    telemetry::attributes::SYSTEM_INSTRUCTIONS,
                    system_instructions.to_string(),
                ));
            }

            let input_messages = telemetry::serialize_input_messages(&request);
            span.set_attribute(opentelemetry::KeyValue::new(
                telemetry::attributes::INPUT_MESSAGES,
                input_messages.to_string(),
            ));
        }

        let start = std::time::Instant::now();

        let result: SdkResult<StreamHandle> = async {
            validate_request(&request)?;
            let model = normalize_model(&request.model)?;
            let payload = GeminiPayload::from_request(&request)?;
            let url = self.stream_endpoint(&model);

            let response = self
                .request(&url)
                .header("accept", "text/event-stream")
                .json(&payload)
                .send()
                .await
                .map_err(|err| {
                    SdkError::Other(anyhow!("Google streaming request failed: {err}"))
                })?;

            let response = ensure_success(response).await?;
            let stream = build_stream(response, model, request.config.response_format.clone());
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
            message: "model must be provided for Google requests".to_string(),
            field: Some("model".to_string()),
        });
    }

    if request.system_prompt.is_none() && request.messages.is_empty() {
        return Err(SdkError::Configuration {
            message: "at least a system prompt or one message is required for Google requests"
                .to_string(),
            field: None,
        });
    }

    Ok(())
}

fn normalize_model(model: &str) -> SdkResult<String> {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return Err(SdkError::Configuration {
            message: "model id must not be empty for Google requests".to_string(),
            field: Some("model".to_string()),
        });
    }

    // Strip google/ prefix if present
    if let Some((provider, rest)) = trimmed.split_once('/') {
        let rest = rest.trim();
        if provider != "google" {
            return Err(SdkError::Configuration {
                message: format!(
                    "Google provider expects model ids prefixed with `google/`; got `{provider}`"
                ),
                field: Some("model".to_string()),
            });
        }
        if rest.is_empty() {
            return Err(SdkError::Configuration {
                message: "model id must be provided after `google/` prefix".to_string(),
                field: Some("model".to_string()),
            });
        }
        Ok(rest.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn build_stream(
    response: reqwest::Response,
    model: String,
    response_format: ResponseFormat,
) -> Pin<Box<dyn futures::Stream<Item = SdkResult<StreamChunk>> + Send>> {
    let bytes_stream = response.bytes_stream();

    let stream = try_stream! {
        futures::pin_mut!(bytes_stream);
        let mut decoder = SseDecoder::default();
        let mut aggregate = String::new();
        let mut partial = PartialResponse::new(model);
        let mut block_started = false;

        while let Some(chunk) = bytes_stream.next().await {
            let chunk = chunk.map_err(|err| SdkError::Other(anyhow!("error reading streaming chunk: {err}")))?;
            for event in decoder.ingest(chunk.as_ref())? {
                let trimmed = event.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let parsed: GeminiStreamResponse = serde_json::from_str(trimmed)
                    .map_err(|err| SdkError::Other(anyhow!("failed to parse Google stream event: {err}")))?;

                // Update usage info
                if let Some(usage) = parsed.usage_metadata {
                    partial.usage = Some(usage);
                }

                // Process candidates
                if let Some(candidates) = parsed.candidates {
                    for candidate in candidates {
                        // Track finish reason
                        if let Some(reason) = candidate.finish_reason {
                            partial.finish_reason = Some(reason);
                        }

                        // Process content
                        if let Some(content) = candidate.content {
                            for part in content.parts {
                                if let Some(text) = part.text {
                                    if !text.is_empty() {
                                        // Start content block on first text
                                        if !block_started {
                                            yield StreamChunk::ContentBlockStart {
                                                index: 0,
                                                block_type: ContentBlockType::Text,
                                            };
                                            block_started = true;
                                        }

                                        aggregate.push_str(&text);
                                        yield StreamChunk::Delta {
                                            content: text,
                                            index: 0,
                                            block_type: ContentBlockType::Text,
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // End the content block
        if block_started {
            yield StreamChunk::ContentBlockStop { index: 0 };
        }

        // Emit completed response
        let response = partial.into_generate_response(aggregate, response_format)?;
        yield StreamChunk::Completed(response);
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
            "Google API error ({status}): {}",
            api_error.error.message
        )));
    }

    Err(SdkError::Other(anyhow!(
        "Google API error ({status}): {body}"
    )))
}

// Request structures
#[derive(Serialize)]
struct GeminiPayload {
    contents: Vec<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<GeminiGenerationConfig>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<GeminiTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_config: Option<GeminiToolConfig>,
}

impl GeminiPayload {
    fn from_request(request: &GenerateRequest) -> SdkResult<Self> {
        let contents = request
            .messages
            .iter()
            .filter(|msg| msg.role != MessageRole::System)
            .map(GeminiContent::from_sdk_message)
            .collect();

        let system_instruction = request.system_prompt.as_ref().map(|prompt| GeminiContent {
            role: Some("user".to_string()), // System instructions use user role in Gemini
            parts: vec![GeminiPart {
                text: Some(prompt.clone()),
                function_call: None,
                function_response: None,
            }],
        });

        let GenerationConfig {
            temperature,
            top_p,
            max_output_tokens,
            response_format,
            reasoning_effort: _,
            modalities: _,
            built_in_tools: _,
        } = request.config.clone();

        let generation_config = Some(GeminiGenerationConfig {
            temperature,
            top_p,
            max_output_tokens: Some(max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS)),
            response_mime_type: response_mime_type(&response_format),
            response_schema: response_schema(&response_format),
        });

        let tools = convert_tools(&request.tools)?;
        let tool_config = convert_tool_choice(request.tool_choice.as_ref());

        Ok(Self {
            contents,
            system_instruction,
            generation_config,
            tools,
            tool_config,
        })
    }
}

#[derive(Serialize)]
struct GeminiContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    parts: Vec<GeminiPart>,
}

impl GeminiContent {
    fn from_sdk_message(message: &Message) -> Self {
        let mut parts = Vec::new();

        // Tool result message (functionResponse)
        if let Some(tool_call_id) = &message.tool_call_id {
            // Parse the result content as JSON if possible, otherwise use as text
            let response_value: JsonValue = serde_json::from_str(&message.content)
                .unwrap_or_else(|_| json!({"result": message.content.clone()}));

            parts.push(GeminiPart {
                text: None,
                function_call: None,
                function_response: Some(GeminiFunctionResponse {
                    name: tool_call_id.clone(), // Use tool_call_id as function name
                    response: response_value,
                }),
            });

            return Self {
                role: Some("user".to_string()), // Function responses are user role
                parts,
            };
        }

        // Assistant message with tool calls (functionCall)
        if let Some(tool_calls) = &message.tool_calls {
            // Add text content if present
            if !message.content.is_empty() {
                parts.push(GeminiPart {
                    text: Some(message.content.clone()),
                    function_call: None,
                    function_response: None,
                });
            }

            // Add function calls
            for tc in tool_calls {
                let args: JsonValue = serde_json::from_str(&tc.arguments)
                    .unwrap_or_else(|_| json!({}));
                parts.push(GeminiPart {
                    text: None,
                    function_call: Some(GeminiFunctionCall {
                        name: tc.name.clone(),
                        args,
                    }),
                    function_response: None,
                });
            }

            return Self {
                role: Some("model".to_string()),
                parts,
            };
        }

        // Regular message
        let role = match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "model",
            MessageRole::System => "user", // System handled separately
        };

        parts.push(GeminiPart {
            text: Some(message.content.clone()),
            function_call: None,
            function_response: None,
        });

        Self {
            role: Some(role.to_string()),
            parts,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct GeminiPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(rename = "functionCall", skip_serializing_if = "Option::is_none")]
    function_call: Option<GeminiFunctionCall>,
    #[serde(rename = "functionResponse", skip_serializing_if = "Option::is_none")]
    function_response: Option<GeminiFunctionResponse>,
}

#[derive(Serialize, Deserialize)]
struct GeminiFunctionCall {
    name: String,
    args: JsonValue,
}

#[derive(Serialize, Deserialize)]
struct GeminiFunctionResponse {
    name: String,
    response: JsonValue,
}

#[derive(Serialize)]
struct GeminiGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_schema: Option<JsonValue>,
}

#[derive(Serialize)]
struct GeminiTool {
    function_declarations: Vec<GeminiFunctionDeclaration>,
}

#[derive(Serialize)]
struct GeminiFunctionDeclaration {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<JsonValue>,
}

#[derive(Serialize)]
struct GeminiToolConfig {
    function_calling_config: GeminiFunctionCallingConfig,
}

#[derive(Serialize)]
struct GeminiFunctionCallingConfig {
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    allowed_function_names: Option<Vec<String>>,
}

fn response_mime_type(format: &ResponseFormat) -> Option<String> {
    match format {
        ResponseFormat::Text => None,
        ResponseFormat::Json | ResponseFormat::JsonSchema(_) => {
            Some("application/json".to_string())
        }
    }
}

fn response_schema(format: &ResponseFormat) -> Option<JsonValue> {
    match format {
        ResponseFormat::JsonSchema(schema) => Some(schema.schema.clone()),
        _ => None,
    }
}

fn convert_tools(tools: &[ToolDefinition]) -> SdkResult<Vec<GeminiTool>> {
    if tools.is_empty() {
        return Ok(Vec::new());
    }

    let function_declarations: Vec<GeminiFunctionDeclaration> = tools
        .iter()
        .map(|tool| GeminiFunctionDeclaration {
            name: tool.name.clone(),
            description: tool.description.clone(),
            parameters: tool.parameters.clone(),
        })
        .collect();

    Ok(vec![GeminiTool {
        function_declarations,
    }])
}

fn convert_tool_choice(choice: Option<&ToolChoice>) -> Option<GeminiToolConfig> {
    match choice {
        None => None,
        Some(ToolChoice::Auto) => Some(GeminiToolConfig {
            function_calling_config: GeminiFunctionCallingConfig {
                mode: "AUTO".to_string(),
                allowed_function_names: None,
            },
        }),
        Some(ToolChoice::None) => Some(GeminiToolConfig {
            function_calling_config: GeminiFunctionCallingConfig {
                mode: "NONE".to_string(),
                allowed_function_names: None,
            },
        }),
        Some(ToolChoice::Required) => Some(GeminiToolConfig {
            function_calling_config: GeminiFunctionCallingConfig {
                mode: "ANY".to_string(), // Forces tool use (any tool)
                allowed_function_names: None,
            },
        }),
        Some(ToolChoice::Tool { name }) => Some(GeminiToolConfig {
            function_calling_config: GeminiFunctionCallingConfig {
                mode: "ANY".to_string(),
                allowed_function_names: Some(vec![name.clone()]),
            },
        }),
    }
}

// Response structures
#[derive(Deserialize, Serialize)]
struct GeminiResponse {
    candidates: Option<Vec<GeminiCandidate>>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsageMetadata>,
}

impl GeminiResponse {
    fn into_generate_response(
        self,
        model: &str,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let raw = serde_json::to_value(&self).ok();

        let mut text = String::new();
        let mut finish_reason = None;

        if let Some(candidates) = &self.candidates {
            if let Some(candidate) = candidates.first() {
                finish_reason = candidate.finish_reason.clone();
                if let Some(content) = &candidate.content {
                    for part in &content.parts {
                        if let Some(t) = &part.text {
                            text.push_str(t);
                        }
                    }
                }
            }
        }

        let object = match &response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json | ResponseFormat::JsonSchema(_) => {
                Some(parse_json_value(&text)?)
            }
        };

        let usage = self.usage_metadata.map(|u| TokenUsage {
            prompt_tokens: u.prompt_token_count,
            completion_tokens: u.candidates_token_count,
            total_tokens: u.total_token_count,
        });

        Ok(GenerateResponse {
            id: uuid::Uuid::new_v4().to_string(),
            model: model.to_string(),
            created: None,
            text,
            usage,
            finish_reason,
            tool_calls: None,
            object,
            raw,
        })
    }
}

#[derive(Deserialize, Serialize)]
struct GeminiCandidate {
    content: Option<GeminiContentResponse>,
    #[serde(rename = "finishReason")]
    finish_reason: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct GeminiContentResponse {
    parts: Vec<GeminiPart>,
}

#[derive(Deserialize, Serialize, Clone)]
struct GeminiUsageMetadata {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: Option<u32>,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: Option<u32>,
    #[serde(rename = "totalTokenCount")]
    total_token_count: Option<u32>,
}

// Streaming response
#[derive(Deserialize)]
struct GeminiStreamResponse {
    candidates: Option<Vec<GeminiCandidate>>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsageMetadata>,
}

#[derive(Deserialize)]
struct ApiError {
    error: ApiErrorDetail,
}

#[derive(Deserialize)]
struct ApiErrorDetail {
    message: String,
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

#[derive(Clone)]
struct PartialResponse {
    model: String,
    usage: Option<GeminiUsageMetadata>,
    finish_reason: Option<String>,
}

impl PartialResponse {
    fn new(model: String) -> Self {
        Self {
            model,
            usage: None,
            finish_reason: None,
        }
    }

    fn into_generate_response(
        self,
        text: String,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let object = match &response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json | ResponseFormat::JsonSchema(_) => {
                if text.trim().is_empty() {
                    None
                } else {
                    Some(parse_json_value(&text)?)
                }
            }
        };

        let usage = self.usage.map(|u| TokenUsage {
            prompt_tokens: u.prompt_token_count,
            completion_tokens: u.candidates_token_count,
            total_tokens: u.total_token_count,
        });

        Ok(GenerateResponse {
            id: uuid::Uuid::new_v4().to_string(),
            model: self.model,
            created: None,
            text,
            usage,
            finish_reason: self.finish_reason,
            tool_calls: None,
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
