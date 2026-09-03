use std::collections::HashMap;
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
    ReasoningEffort, ResponseFormat, StreamChunk, StreamHandle, StreamRequest, TokenUsage,
    ToolCall, ToolChoice, ToolDefinition,
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
    pub retry_config: http::RetryConfig,
}

impl GoogleConfig {
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
/// - Gemini 3.6 Flash (stable, tool-capable)
/// - Gemini 3.5 Flash Lite (cost-effective)
///
/// # Example
///
/// ```no_run
/// use agnt5_sdk_core::lm::{GoogleProvider, GenerateRequest};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = GoogleProvider::from_env()?;
/// let response = provider.generate(
///     GenerateRequest::new("google/gemini-3.6-flash")
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
        let http = http::build_http_client(config.timeout)?;

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

    fn cached_contents_endpoint(&self) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        format!(
            "{base}/{}/cachedContents?key={}",
            self.config.version, self.config.api_key
        )
    }

    fn cached_content_resource_endpoint(&self, name: &str) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        let name = name.trim_start_matches('/');
        format!(
            "{base}/{}/{}?key={}",
            self.config.version, name, self.config.api_key
        )
    }

    fn request(&self, url: &str) -> reqwest::RequestBuilder {
        self.http
            .post(url)
            .header("content-type", "application/json")
    }

    fn delete_request(&self, url: &str) -> reqwest::RequestBuilder {
        self.http
            .delete(url)
            .header("content-type", "application/json")
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        generate_via_model(self, request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        stream_via_model(self, request).await
    }

    pub async fn create_cached_content(
        &self,
        model: &str,
        system: Option<String>,
        contents: Vec<String>,
        ttl_seconds: Option<u32>,
    ) -> SdkResult<String> {
        if contents.is_empty() {
            return Err(SdkError::Configuration {
                message: "at least one content item is required for Gemini CachedContent"
                    .to_string(),
                field: Some("contents".to_string()),
            });
        }

        let model = normalize_model(model)?;
        let payload = GeminiCachedContentPayload::new(model, system, contents, ttl_seconds)?;
        let url = self.cached_contents_endpoint();

        let response = http::send_with_retry(
            || self.request(&url).json(&payload),
            &self.config.retry_config,
            "google",
            None,
        )
        .await?;

        let parsed: GeminiCachedContentResponse = response.json().await.map_err(|err| {
            SdkError::Other(anyhow!(
                "failed to parse Google CachedContent response: {err}"
            ))
        })?;

        Ok(parsed.name)
    }

    pub async fn delete_cached_content(&self, name: &str) -> SdkResult<()> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(SdkError::Configuration {
                message: "Gemini CachedContent name must not be empty".to_string(),
                field: Some("name".to_string()),
            });
        }

        let url = self.cached_content_resource_endpoint(trimmed);
        http::send_with_retry(
            || self.delete_request(&url),
            &self.config.retry_config,
            "google",
            None,
        )
        .await?;

        Ok(())
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

        let result: SdkResult<GenerateResponse> = async {
            validate_request(&request)?;
            let model = normalize_model(&request.model)?;
            let payload = GeminiPayload::from_request(&request)?;
            let url = self.generate_endpoint(&model);

            let response = http::send_with_retry(
                || self.request(&url).json(&payload),
                &self.config.retry_config,
                "google",
                request.config.timeout,
            )
            .await?;

            let metadata = http::extract_metadata(&response);
            let parsed: GeminiResponse = response.json().await.map_err(|err| {
                SdkError::Other(anyhow!("failed to parse Google response: {err}"))
            })?;

            let mut result =
                parsed.into_generate_response(&model, request.config.response_format.clone())?;
            result.metadata = Some(metadata);
            Ok(result)
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
                            usage.cached_tokens,
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

            let response = http::send_with_retry(
                || {
                    self.request(&url)
                        .header("accept", "text/event-stream")
                        .json(&payload)
                },
                &self.config.retry_config,
                "google",
                request.config.timeout,
            )
            .await?;
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

                for text in partial.absorb(parsed) {
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

// Request structures
#[derive(Serialize)]
struct GeminiPayload {
    contents: Vec<GeminiContent>,
    #[serde(rename = "cachedContent", skip_serializing_if = "Option::is_none")]
    cached_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<GeminiGenerationConfig>,
    // Mixed list: function-declaration tool entries and provider-hosted built-ins
    // (e.g. `{"google_search": {}}` for Gemini 2.0+).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_config: Option<GeminiToolConfig>,
}

impl GeminiPayload {
    fn from_request(request: &GenerateRequest) -> SdkResult<Self> {
        let mut contents = Vec::new();
        let mut tool_call_names = HashMap::new();
        for message in request
            .messages
            .iter()
            .filter(|msg| msg.role != MessageRole::System)
        {
            if let Some(tool_calls) = &message.tool_calls {
                for tool_call in tool_calls {
                    tool_call_names.insert(tool_call.id.clone(), tool_call.name.clone());
                }
            }

            let function_name = message
                .tool_call_id
                .as_ref()
                .and_then(|id| tool_call_names.get(id))
                .map(String::as_str);
            contents.push(GeminiContent::from_sdk_message(message, function_name));
        }

        let system_instruction = request.system_prompt.as_ref().map(|prompt| GeminiContent {
            role: Some("user".to_string()), // System instructions use user role in Gemini
            parts: vec![GeminiPart {
                text: Some(prompt.clone()),
                function_call: None,
                function_response: None,
                thought_signature: None,
            }],
        });

        let GenerationConfig {
            temperature,
            top_p,
            max_output_tokens,
            response_format,
            prompt_cache,
            reasoning_effort,
            modalities: _,
            built_in_tools,
            timeout: _,
        } = request.config.clone();

        let generation_config = Some(GeminiGenerationConfig {
            temperature,
            top_p,
            max_output_tokens: Some(max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS)),
            response_mime_type: response_mime_type(&response_format),
            response_json_schema: response_json_schema(&response_format),
            thinking_config: thinking_config(reasoning_effort.as_ref()),
        });

        // Build a mixed tools array: function-declaration tools + Gemini
        // server-side built-ins (google_search today).
        let mut tools: Vec<JsonValue> = convert_tools(&request.tools)?
            .into_iter()
            .map(|t| serde_json::to_value(t).unwrap_or(JsonValue::Null))
            .collect();

        for built_in in &built_in_tools {
            if let Some(spec) = gemini_built_in_spec(built_in) {
                tools.push(spec);
            }
        }

        let tool_config = convert_tool_choice(request.tool_choice.as_ref());

        Ok(Self {
            contents,
            cached_content: prompt_cache.and_then(|cache| {
                cache.resource.and_then(|resource| {
                    let trimmed = resource.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
            }),
            system_instruction,
            generation_config,
            tools,
            tool_config,
        })
    }
}

#[derive(Serialize)]
struct GeminiCachedContentPayload {
    model: String,
    #[serde(rename = "systemInstruction", skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiContent>,
    contents: Vec<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl: Option<String>,
}

impl GeminiCachedContentPayload {
    fn new(
        model: String,
        system: Option<String>,
        contents: Vec<String>,
        ttl_seconds: Option<u32>,
    ) -> SdkResult<Self> {
        let model = if model.starts_with("models/") {
            model
        } else {
            format!("models/{model}")
        };

        let system_instruction = system.and_then(|prompt| {
            let trimmed = prompt.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(GeminiContent::from_text(None, trimmed.to_string()))
            }
        });

        let mut cache_contents = Vec::with_capacity(contents.len());
        for content in contents {
            let trimmed = content.trim();
            if trimmed.is_empty() {
                continue;
            }
            cache_contents.push(GeminiContent::from_text(
                Some("user".to_string()),
                trimmed.to_string(),
            ));
        }

        if cache_contents.is_empty() {
            return Err(SdkError::Configuration {
                message: "Gemini CachedContent contents must include non-empty text".to_string(),
                field: Some("contents".to_string()),
            });
        }

        let ttl = ttl_seconds.map(|seconds| format!("{seconds}s"));

        Ok(Self {
            model,
            system_instruction,
            contents: cache_contents,
            ttl,
        })
    }
}

#[derive(Deserialize)]
struct GeminiCachedContentResponse {
    name: String,
}

/// Map a generic BuiltInTool to its Gemini API tool spec. Gemini 2.0+ accepts
/// `{"google_search": {}}` to enable server-side grounding. Returns None for
/// variants Gemini does not host server-side.
fn gemini_built_in_spec(tool: &BuiltInTool) -> Option<JsonValue> {
    match tool {
        BuiltInTool::WebSearch => Some(json!({"google_search": {}})),
        BuiltInTool::CodeInterpreter | BuiltInTool::FileSearch | BuiltInTool::WebFetch => None,
    }
}

#[derive(Serialize)]
struct GeminiContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    parts: Vec<GeminiPart>,
}

impl GeminiContent {
    fn from_text(role: Option<String>, text: String) -> Self {
        Self {
            role,
            parts: vec![GeminiPart {
                text: Some(text),
                function_call: None,
                function_response: None,
                thought_signature: None,
            }],
        }
    }

    fn from_sdk_message(message: &Message, function_name: Option<&str>) -> Self {
        let mut parts = Vec::new();

        // Tool result message (functionResponse)
        if let Some(tool_call_id) = &message.tool_call_id {
            // Parse the result content as JSON if possible, otherwise use as text
            let response_value = google_function_response(&message.content);

            parts.push(GeminiPart {
                text: None,
                function_call: None,
                function_response: Some(GeminiFunctionResponse {
                    name: function_name.unwrap_or(tool_call_id).to_string(),
                    response: response_value,
                    id: Some(tool_call_id.clone()),
                }),
                thought_signature: None,
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
                    thought_signature: None,
                });
            }

            // Add function calls
            for tc in tool_calls {
                let args: JsonValue =
                    serde_json::from_str(&tc.arguments).unwrap_or_else(|_| json!({}));
                parts.push(GeminiPart {
                    text: None,
                    function_call: Some(GeminiFunctionCall {
                        id: Some(tc.id.clone()),
                        name: tc.name.clone(),
                        args,
                    }),
                    function_response: None,
                    thought_signature: google_thought_signature(tc).map(str::to_owned),
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
            thought_signature: None,
        });

        Self {
            role: Some(role.to_string()),
            parts,
        }
    }
}

fn google_function_response(content: &str) -> JsonValue {
    match serde_json::from_str(content) {
        Ok(value @ JsonValue::Object(_)) => value,
        Ok(value) => json!({"result": value}),
        Err(_) => json!({"result": content}),
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
    #[serde(rename = "thoughtSignature", skip_serializing_if = "Option::is_none")]
    thought_signature: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct GeminiFunctionCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    name: String,
    args: JsonValue,
}

impl GeminiPart {
    fn to_tool_call(&self, index: usize) -> Option<ToolCall> {
        self.function_call.as_ref().map(|function_call| ToolCall {
            // Gemini functionCall parts do not include IDs. Match the fallback
            // convention used by the OpenAI-compatible streaming parser.
            id: function_call
                .id
                .clone()
                .unwrap_or_else(|| format!("call_{index}")),
            name: function_call.name.clone(),
            arguments: function_call.args.to_string(),
            provider_data: self
                .thought_signature
                .as_ref()
                .map(|signature| json!({"google": {"thought_signature": signature}})),
        })
    }
}

fn google_thought_signature(tool_call: &ToolCall) -> Option<&str> {
    tool_call
        .provider_data
        .as_ref()?
        .get("google")?
        .get("thought_signature")?
        .as_str()
}

#[derive(Serialize, Deserialize)]
struct GeminiFunctionResponse {
    name: String,
    response: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
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
    #[serde(rename = "responseJsonSchema", skip_serializing_if = "Option::is_none")]
    response_json_schema: Option<JsonValue>,
    #[serde(rename = "thinkingConfig", skip_serializing_if = "Option::is_none")]
    thinking_config: Option<GeminiThinkingConfig>,
}

#[derive(Serialize)]
struct GeminiThinkingConfig {
    #[serde(rename = "thinkingLevel")]
    thinking_level: &'static str,
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

fn response_json_schema(format: &ResponseFormat) -> Option<JsonValue> {
    match format {
        ResponseFormat::JsonSchema(schema) => Some(schema.schema.clone()),
        _ => None,
    }
}

fn thinking_config(effort: Option<&ReasoningEffort>) -> Option<GeminiThinkingConfig> {
    effort.map(|effort| GeminiThinkingConfig {
        thinking_level: match effort {
            ReasoningEffort::Minimal => "minimal",
            ReasoningEffort::Medium => "medium",
            ReasoningEffort::High => "high",
        },
    })
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
        let mut tool_calls = Vec::new();

        if let Some(candidates) = &self.candidates {
            if let Some(candidate) = candidates.first() {
                finish_reason = candidate.finish_reason.clone();
                if let Some(content) = &candidate.content {
                    for part in &content.parts {
                        if let Some(t) = &part.text {
                            text.push_str(t);
                        }
                        if let Some(tool_call) = part.to_tool_call(tool_calls.len()) {
                            tool_calls.push(tool_call);
                        }
                    }
                }
            }
        }

        let object = match &response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json | ResponseFormat::JsonSchema(_) => Some(parse_json_value(&text)?),
        };

        let usage = self.usage_metadata.map(|u| TokenUsage {
            prompt_tokens: u.prompt_token_count,
            completion_tokens: u.candidates_token_count,
            total_tokens: u.total_token_count,
            cached_tokens: u.cached_content_token_count,
            cache_creation_tokens: None,
        });

        Ok(GenerateResponse {
            id: uuid::Uuid::new_v4().to_string(),
            model: model.to_string(),
            created: None,
            text,
            usage,
            finish_reason,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            object,
            raw,
            metadata: None,
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
    /// Tokens served from Gemini context caching. Subset of `promptTokenCount`.
    #[serde(rename = "cachedContentTokenCount", default)]
    cached_content_token_count: Option<u32>,
}

// Streaming response
#[derive(Deserialize)]
struct GeminiStreamResponse {
    candidates: Option<Vec<GeminiCandidate>>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsageMetadata>,
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
    tool_calls: Vec<ToolCall>,
}

impl PartialResponse {
    fn new(model: String) -> Self {
        Self {
            model,
            usage: None,
            finish_reason: None,
            tool_calls: Vec::new(),
        }
    }

    fn absorb(&mut self, response: GeminiStreamResponse) -> Vec<String> {
        if let Some(usage) = response.usage_metadata {
            self.usage = Some(usage);
        }

        let mut text_parts = Vec::new();
        if let Some(candidates) = response.candidates {
            for candidate in candidates {
                if let Some(reason) = candidate.finish_reason {
                    self.finish_reason = Some(reason);
                }

                if let Some(content) = candidate.content {
                    for part in content.parts {
                        if let Some(tool_call) = part.to_tool_call(self.tool_calls.len()) {
                            self.tool_calls.push(tool_call);
                        }
                        if let Some(text) = part.text {
                            text_parts.push(text);
                        }
                    }
                }
            }
        }

        text_parts
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
            cached_tokens: u.cached_content_token_count,
            cache_creation_tokens: None,
        });

        Ok(GenerateResponse {
            id: uuid::Uuid::new_v4().to_string(),
            model: self.model,
            created: None,
            text,
            usage,
            finish_reason: self.finish_reason,
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

#[derive(Default)]
struct SseDecoder {
    buffer: String,
    incomplete_utf8: Vec<u8>,
}

impl SseDecoder {
    fn ingest(&mut self, chunk: &[u8]) -> SdkResult<Vec<String>> {
        super::sse::append_utf8(&mut self.buffer, &mut self.incomplete_utf8, chunk)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_payload_includes_cached_content_reference() {
        let request = GenerateRequest::new("google/gemini-2.5-flash")
            .user_message("What are the termination clauses?")
            .configure(|config| {
                config.prompt_cache = Some(
                    crate::lm::PromptCacheConfig::enabled().resource("cachedContents/cache_123"),
                );
            });

        let payload = GeminiPayload::from_request(&request).unwrap();
        let value = serde_json::to_value(payload).unwrap();

        assert_eq!(value["cachedContent"], "cachedContents/cache_123");
        assert!(value.get("contents").is_some());
    }

    #[test]
    fn generate_payload_uses_json_schema_and_gemini_thinking_level() {
        let request = GenerateRequest::new("google/gemini-3.6-flash")
            .user_message("Return city facts.")
            .response_format(ResponseFormat::JsonSchema(
                crate::lm::JsonSchemaFormat::new(
                    "city_facts",
                    json!({
                        "type": "object",
                        "properties": {"city": {"type": "string"}},
                        "required": ["city"],
                        "additionalProperties": false
                    }),
                ),
            ))
            .configure(|config| {
                config.reasoning_effort = Some(crate::lm::ReasoningEffort::Minimal);
            });

        let payload = GeminiPayload::from_request(&request).unwrap();
        let value = serde_json::to_value(payload).unwrap();
        let generation_config = &value["generation_config"];

        assert!(generation_config.get("response_schema").is_none());
        assert_eq!(
            generation_config["responseJsonSchema"]["additionalProperties"],
            false
        );
        assert_eq!(
            generation_config["thinkingConfig"]["thinkingLevel"],
            "minimal"
        );
    }

    #[test]
    fn cached_content_create_payload_uses_models_prefix_and_ttl() {
        let payload = GeminiCachedContentPayload::new(
            "gemini-2.5-flash".to_string(),
            Some("You are a legal analyst.".to_string()),
            vec!["Large stable document".to_string()],
            Some(3600),
        )
        .unwrap();

        let value = serde_json::to_value(payload).unwrap();

        assert_eq!(value["model"], "models/gemini-2.5-flash");
        assert_eq!(value["ttl"], "3600s");
        assert_eq!(
            value["systemInstruction"]["parts"][0]["text"],
            "You are a legal analyst."
        );
        assert_eq!(
            value["contents"][0]["parts"][0]["text"],
            "Large stable document"
        );
    }

    #[test]
    fn non_streaming_response_extracts_function_calls() {
        let fixture = json!({
            "candidates": [{
                "content": {
                    "parts": [
                        {"text": "I'll check."},
                        {
                            "functionCall": {
                                "id": "google-call-123",
                                "name": "lookup_weather",
                                "args": {"city": "San Francisco"}
                            },
                            "thoughtSignature": "opaque-google-signature"
                        }
                    ]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {
                "promptTokenCount": 12,
                "candidatesTokenCount": 4,
                "totalTokenCount": 16
            }
        });
        let response: GeminiResponse = serde_json::from_value(fixture).unwrap();

        let response = response
            .into_generate_response("gemini-2.5-flash", ResponseFormat::Text)
            .unwrap();
        let tool_calls = response.tool_calls.unwrap();

        assert_eq!(response.text, "I'll check.");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "google-call-123");
        assert_eq!(tool_calls[0].name, "lookup_weather");
        assert_eq!(tool_calls[0].arguments, r#"{"city":"San Francisco"}"#);
        assert_eq!(
            tool_calls[0].provider_data,
            Some(json!({"google": {"thought_signature": "opaque-google-signature"}}))
        );
    }

    #[test]
    fn google_thought_signature_survives_tool_call_replay() {
        let tool_call = ToolCall {
            id: "call_0".to_string(),
            name: "calculate".to_string(),
            arguments: r#"{"expression":"15 * 23"}"#.to_string(),
            provider_data: Some(
                json!({"google": {"thought_signature": "opaque-google-signature"}}),
            ),
        };
        let request = GenerateRequest::new("google/gemini-3.5-flash-lite")
            .message(Message::assistant_with_tool_calls("", vec![tool_call]))
            .message(Message::tool_result("call_0", "345"));

        let payload = GeminiPayload::from_request(&request).unwrap();
        let value = serde_json::to_value(payload).unwrap();

        assert_eq!(
            value["contents"][0]["parts"][0]["thoughtSignature"],
            "opaque-google-signature"
        );
    }

    #[test]
    fn streaming_response_accumulates_function_calls_in_completed_response() {
        let fixture = json!({
            "candidates": [{
                "content": {
                    "parts": [{
                        "functionCall": {
                            "id": "google-call-456",
                            "name": "calculate",
                            "args": {"expression": "15 * 7"}
                        }
                    }]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 3,
                "totalTokenCount": 13
            }
        });
        let event: GeminiStreamResponse = serde_json::from_value(fixture).unwrap();
        let mut partial = PartialResponse::new("gemini-2.5-flash".to_string());

        let text = partial.absorb(event).join("");
        let response = partial
            .into_generate_response(text, ResponseFormat::Text)
            .unwrap();
        let tool_calls = response.tool_calls.unwrap();

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "google-call-456");
        assert_eq!(tool_calls[0].name, "calculate");
        assert_eq!(tool_calls[0].arguments, r#"{"expression":"15 * 7"}"#);
        assert_eq!(response.finish_reason.as_deref(), Some("STOP"));
    }

    #[test]
    fn text_only_response_keeps_tool_calls_empty() {
        let fixture = json!({
            "candidates": [{
                "content": {"parts": [{"text": "No tool needed."}]},
                "finishReason": "STOP"
            }]
        });
        let response: GeminiResponse = serde_json::from_value(fixture).unwrap();

        let response = response
            .into_generate_response("gemini-2.5-flash", ResponseFormat::Text)
            .unwrap();

        assert_eq!(response.text, "No tool needed.");
        assert!(response.tool_calls.is_none());
    }

    #[test]
    fn tool_result_resolves_synthetic_id_back_to_function_name() {
        let request = GenerateRequest::new("google/gemini-2.5-flash")
            .message(Message::assistant_with_tool_calls(
                "",
                vec![ToolCall {
                    id: "call_0".to_string(),
                    name: "calculate".to_string(),
                    arguments: r#"{"expression":"15 * 7"}"#.to_string(),
                    provider_data: None,
                }],
            ))
            .message(Message::tool_result("call_0", r#"{"value":105}"#));

        let payload = GeminiPayload::from_request(&request).unwrap();
        let value = serde_json::to_value(payload).unwrap();

        assert_eq!(
            value["contents"][1]["parts"][0]["functionResponse"]["name"],
            "calculate"
        );
        assert_eq!(
            value["contents"][1]["parts"][0]["functionResponse"]["id"],
            "call_0"
        );
        assert_eq!(
            value["contents"][1]["parts"][0]["functionResponse"]["response"]["value"],
            105
        );
    }

    #[test]
    fn scalar_tool_result_is_wrapped_as_a_google_struct() {
        let request = GenerateRequest::new("google/gemini-3.5-flash-lite")
            .message(Message::assistant_with_tool_calls(
                "",
                vec![ToolCall {
                    id: "call_0".to_string(),
                    name: "calculate".to_string(),
                    arguments: r#"{"expression":"15 * 23"}"#.to_string(),
                    provider_data: None,
                }],
            ))
            .message(Message::tool_result("call_0", "345"));

        let payload = GeminiPayload::from_request(&request).unwrap();
        let value = serde_json::to_value(payload).unwrap();

        assert_eq!(
            value["contents"][1]["parts"][0]["functionResponse"]["response"],
            json!({"result": 345})
        );
    }
}
