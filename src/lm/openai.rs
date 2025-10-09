use std::env;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use opentelemetry::trace::{Span, TraceContextExt};
use reqwest::Client;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    generate as generate_via_model, stream as stream_via_model, GenerateRequest, GenerateResponse,
    LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_common::{
    parse_error, stream_handle_from_response, ChatCompletionPayload, ChatCompletionResponse,
};
use super::telemetry;

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_CHAT_PATH: &str = "chat/completions";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MODEL_PREFIX: &str = "openai";

/// Configuration for OpenAI-compatible chat completion endpoints.
#[derive(Clone, Debug)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub base_url: String,
    pub chat_path: String,
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
            chat_path: DEFAULT_CHAT_PATH.to_string(),
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

    pub fn with_chat_path(mut self, chat_path: impl Into<String>) -> Self {
        self.chat_path = chat_path.into();
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

        if let Ok(chat_path) = env::var("OPENAI_CHAT_PATH") {
            if !chat_path.trim().is_empty() {
                config.chat_path = chat_path;
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

/// Minimal provider implementation for OpenAI chat completions.
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
        let path = self.config.chat_path.trim_start_matches('/');
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
                "model id must not be empty for OpenAI requests".to_string(),
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

#[async_trait]
impl LanguageModel for OpenAiProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        // Create OpenTelemetry span for this LLM call (as child of provided or current context)
        let mut span = telemetry::create_gen_ai_span("openai", &request.model, request.otel_context.clone());

        if let Some(ref ctx) = request.otel_context {
            let span_ref = ctx.span();
            let span_ctx = span_ref.span_context();
            tracing::info!(
                "Trace propagation: REQUEST context trace_id={}, span_id={}, valid={}",
                span_ctx.trace_id().to_string(),
                span_ctx.span_id().to_string(),
                span_ctx.is_valid()
            );
        } else {
            tracing::warn!("Trace propagation: REQUEST context missing for LLM call");
        }

        {
            let span_ctx = span.span_context();
            tracing::info!(
                "Trace propagation: LLM span trace_id={}, span_id={}, valid={}",
                span_ctx.trace_id().to_string(),
                span_ctx.span_id().to_string(),
                span_ctx.is_valid()
            );
        }

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

        // Execute the actual API call (span is already linked to parent via create_gen_ai_span)
        let result = async {
            validate_request(&request)?;
            let model = self.normalize_model(&request.model)?;
            let payload = ChatCompletionPayload::from_request(&request, model, false);

            let response = self
                .request()
                .json(&payload)
                .send()
                .await
                .map_err(|err| SdkError::Other(anyhow!("OpenAI request failed: {err}")))?;

            let response = ensure_success(response).await?;

            let parsed: ChatCompletionResponse = response
                .json()
                .await
                .map_err(|err| SdkError::Other(anyhow!("failed to parse OpenAI response: {err}")))?;

            parsed.into_generate_response(request.config.response_format.clone())
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
        // Create OpenTelemetry span for this streaming LLM call (as child of provided or current context)
        let mut span = telemetry::create_gen_ai_span("openai", &request.model, request.otel_context.clone());

        if let Some(ref ctx) = request.otel_context {
            let span_ref = ctx.span();
            let span_ctx = span_ref.span_context();
            tracing::info!(
                "Trace propagation: STREAM REQUEST context trace_id={}, span_id={}, valid={}",
                span_ctx.trace_id().to_string(),
                span_ctx.span_id().to_string(),
                span_ctx.is_valid()
            );
        } else {
            tracing::warn!("Trace propagation: STREAM REQUEST context missing for LLM call");
        }

        {
            let span_ctx = span.span_context();
            tracing::info!(
                "Trace propagation: STREAM LLM span trace_id={}, span_id={}, valid={}",
                span_ctx.trace_id().to_string(),
                span_ctx.span_id().to_string(),
                span_ctx.is_valid()
            );
        }

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

        // Execute the actual streaming API call (span is already linked to parent via create_gen_ai_span)
        let result = async {
            validate_request(&request)?;
            let model = self.normalize_model(&request.model)?;
            let payload = ChatCompletionPayload::from_request(&request, model, true);

            let response = self
                .request()
                .header("Accept", "text/event-stream")
                .json(&payload)
                .send()
                .await
                .map_err(|err| SdkError::Other(anyhow!("OpenAI streaming request failed: {err}")))?;

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

fn validate_request(request: &GenerateRequest) -> SdkResult<()> {
    if request.system_prompt.is_none() && request.messages.is_empty() {
        return Err(SdkError::Configuration(
            "at least a system prompt or one message is required for OpenAI requests".to_string(),
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
            "OpenAI API error ({status}): {message}"
        )));
    }

    Err(SdkError::Other(anyhow!(
        "OpenAI API error ({status}): {body}"
    )))
}
