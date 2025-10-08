/// OpenTelemetry instrumentation helpers for LLM/Gen AI operations
///
/// This module provides utilities for instrumenting language model API calls
/// following the OpenTelemetry Semantic Conventions for Generative AI:
/// https://opentelemetry.io/docs/specs/semconv/gen-ai/
///
/// Key features:
/// - Span creation with proper naming: "chat {model}"
/// - Required and recommended Gen AI attributes
/// - Optional content capture (opt-in via environment variable)
/// - Structured message serialization per OpenTelemetry format
/// - Support for streaming responses with span events

use opentelemetry::trace::{Span, SpanKind, Status, Tracer};
use opentelemetry::{global, KeyValue};
use serde_json::{json, Value};

use super::interface::{GenerateRequest, GenerateResponse, MessageRole, TokenUsage};

/// Semantic convention attribute names for Gen AI operations
pub mod attributes {
    pub const OPERATION_NAME: &str = "gen_ai.operation.name";
    pub const PROVIDER_NAME: &str = "gen_ai.provider.name";
    pub const REQUEST_MODEL: &str = "gen_ai.request.model";
    pub const RESPONSE_MODEL: &str = "gen_ai.response.model";
    pub const REQUEST_TEMPERATURE: &str = "gen_ai.request.temperature";
    pub const REQUEST_TOP_P: &str = "gen_ai.request.top_p";
    pub const REQUEST_MAX_TOKENS: &str = "gen_ai.request.max_tokens";
    pub const USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";
    pub const USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";
    pub const RESPONSE_FINISH_REASONS: &str = "gen_ai.response.finish_reasons";
    pub const INPUT_MESSAGES: &str = "gen_ai.input.messages";
    pub const OUTPUT_MESSAGES: &str = "gen_ai.output.messages";
    pub const SYSTEM_INSTRUCTIONS: &str = "gen_ai.system_instructions";
    pub const REQUEST_DURATION_MS: &str = "gen_ai.request.duration_ms";
}

/// Check if content capture is enabled via environment variable
///
/// Per OpenTelemetry spec, content capture should be OPT-IN for privacy/security.
/// Set AGNT5_LLM_CAPTURE_CONTENT=true to enable full input/output recording.
pub fn should_capture_content() -> bool {
    std::env::var("AGNT5_LLM_CAPTURE_CONTENT")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false)
}

/// Create a Gen AI span with proper naming and attributes
///
/// Creates a span named `chat {model}` with SpanKind::Client per OpenTelemetry spec.
/// Sets required attributes: operation.name, provider.name, request.model
pub fn create_gen_ai_span(provider: &str, model: &str) -> impl Span {
    let tracer = global::tracer("agnt5-sdk-core");

    // Span name format: "{operation_name} {model_name}" per spec
    let span_name = format!("chat {}", model);

    let mut span = tracer
        .span_builder(span_name)
        .with_kind(SpanKind::Client)
        .start(&tracer);

    // Required attributes per OpenTelemetry Gen AI conventions
    span.set_attribute(KeyValue::new(attributes::OPERATION_NAME, "chat"));
    span.set_attribute(KeyValue::new(attributes::PROVIDER_NAME, provider.to_string()));
    span.set_attribute(KeyValue::new(attributes::REQUEST_MODEL, model.to_string()));

    span
}

/// Set request configuration attributes on the span
pub fn set_request_attributes(span: &mut impl Span, request: &GenerateRequest) {
    // Optional configuration parameters
    if let Some(temperature) = request.config.temperature {
        span.set_attribute(KeyValue::new(
            attributes::REQUEST_TEMPERATURE,
            temperature as f64,
        ));
    }

    if let Some(top_p) = request.config.top_p {
        span.set_attribute(KeyValue::new(
            attributes::REQUEST_TOP_P,
            top_p as f64,
        ));
    }

    if let Some(max_tokens) = request.config.max_output_tokens {
        span.set_attribute(KeyValue::new(
            attributes::REQUEST_MAX_TOKENS,
            max_tokens as i64,
        ));
    }
}

/// Serialize input messages to OpenTelemetry Gen AI format if content capture is enabled
///
/// Format per spec:
/// ```json
/// [
///   {
///     "role": "system",
///     "parts": [{"type": "text", "content": "You are helpful"}]
///   },
///   {
///     "role": "user",
///     "parts": [{"type": "text", "content": "Hello"}]
///   }
/// ]
/// ```
pub fn serialize_input_messages(request: &GenerateRequest) -> Value {
    let mut messages_array = Vec::new();

    // Add system message if present
    if let Some(system_prompt) = &request.system_prompt {
        messages_array.push(json!({
            "role": "system",
            "parts": [
                {
                    "type": "text",
                    "content": truncate_content(system_prompt, 10_000)
                }
            ]
        }));
    }

    // Add conversation messages
    for msg in &request.messages {
        messages_array.push(json!({
            "role": role_to_string(&msg.role),
            "parts": [
                {
                    "type": "text",
                    "content": truncate_content(&msg.content, 10_000)
                }
            ]
        }));
    }

    json!(messages_array)
}

/// Serialize output messages to OpenTelemetry Gen AI format
pub fn serialize_output_messages(response: &GenerateResponse) -> Value {
    json!([
        {
            "role": "assistant",
            "parts": [
                {
                    "type": "text",
                    "content": truncate_content(&response.text, 10_000)
                }
            ]
        }
    ])
}

/// Set response attributes on the span
pub fn set_response_attributes(
    span: &mut impl Span,
    response: &GenerateResponse,
    capture_content: bool,
) {
    // Actual model that generated the response (may differ from request model)
    span.set_attribute(KeyValue::new(
        attributes::RESPONSE_MODEL,
        response.model.clone(),
    ));

    // Token usage
    if let Some(usage) = &response.usage {
        set_token_usage_attributes(span, usage);
    }

    // Finish reason
    if let Some(finish_reason) = &response.finish_reason {
        span.set_attribute(KeyValue::new(
            attributes::RESPONSE_FINISH_REASONS,
            format!("[\"{}\"]", finish_reason),
        ));
    }

    // Optional content capture
    if capture_content {
        let output_messages = serialize_output_messages(response);
        span.set_attribute(KeyValue::new(
            attributes::OUTPUT_MESSAGES,
            output_messages.to_string(),
        ));
    }

    // Mark span as successful
    span.set_status(Status::Ok);
}

/// Set token usage attributes on the span
pub fn set_token_usage_attributes(span: &mut impl Span, usage: &TokenUsage) {
    if let Some(prompt_tokens) = usage.prompt_tokens {
        span.set_attribute(KeyValue::new(
            attributes::USAGE_INPUT_TOKENS,
            prompt_tokens as i64,
        ));
    }

    if let Some(completion_tokens) = usage.completion_tokens {
        span.set_attribute(KeyValue::new(
            attributes::USAGE_OUTPUT_TOKENS,
            completion_tokens as i64,
        ));
    }
}

/// Record an error on the span
pub fn set_error_status(span: &mut impl Span, error: &str) {
    span.set_status(Status::error(error.to_string()));
}

/// Record request duration on the span
pub fn set_duration(span: &mut impl Span, duration_ms: u128) {
    span.set_attribute(KeyValue::new(
        attributes::REQUEST_DURATION_MS,
        duration_ms as i64,
    ));
}

/// Truncate content to prevent excessively large span attributes
///
/// Default limit: 10KB per field to avoid overwhelming observability backends
fn truncate_content(content: &str, max_bytes: usize) -> String {
    if content.len() <= max_bytes {
        content.to_string()
    } else {
        format!(
            "{}... [truncated {} bytes]",
            &content[..max_bytes],
            content.len() - max_bytes
        )
    }
}

/// Convert MessageRole to string for serialization
fn role_to_string(role: &MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_capture_content_default_false() {
        // Without env var set, should return false
        assert!(!should_capture_content());
    }

    #[test]
    fn test_truncate_content_short() {
        let content = "Hello, world!";
        assert_eq!(truncate_content(content, 100), "Hello, world!");
    }

    #[test]
    fn test_truncate_content_long() {
        let content = "x".repeat(20000);
        let truncated = truncate_content(&content, 10000);
        assert!(truncated.contains("[truncated"));
        assert!(truncated.len() < content.len());
    }

    #[test]
    fn test_serialize_input_messages() {
        let request = GenerateRequest::new("gpt-4")
            .system_prompt("You are helpful")
            .user_message("Hello");

        let serialized = serialize_input_messages(&request);
        assert!(serialized.is_array());

        let array = serialized.as_array().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array[0]["role"], "system");
        assert_eq!(array[1]["role"], "user");
    }
}
