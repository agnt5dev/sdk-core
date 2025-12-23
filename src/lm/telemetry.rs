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

use super::interface::{GenerateRequest, GenerateResponse, MessageRole, TokenUsage, ToolDefinition};

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
    pub const USAGE_COST: &str = "gen_ai.usage.cost";
    pub const USAGE_COST_CURRENCY: &str = "gen_ai.usage.cost.currency";
    pub const RESPONSE_FINISH_REASONS: &str = "gen_ai.response.finish_reasons";
    pub const INPUT_MESSAGES: &str = "gen_ai.input.messages";
    pub const OUTPUT_MESSAGES: &str = "gen_ai.output.messages";
    pub const SYSTEM_INSTRUCTIONS: &str = "gen_ai.system_instructions";
    pub const REQUEST_DURATION_MS: &str = "gen_ai.request.duration_ms";
}

/// Check if content capture is enabled via environment variable
///
/// Content capture is ENABLED by default to provide full visibility into LLM interactions.
/// Set AGNT5_LLM_CAPTURE_CONTENT=false to disable full input/output recording.
pub fn should_capture_content() -> bool {
    std::env::var("AGNT5_LLM_CAPTURE_CONTENT")
        .map(|v| v.to_lowercase() != "false" && v != "0")
        .unwrap_or(true)
}

/// Model pricing information
///
/// Prices are in USD per 1 million tokens
#[derive(Debug, Clone)]
pub struct ModelPricing {
    pub input_per_1m: f64,
    pub output_per_1m: f64,
}

/// Get model pricing based on provider and model name
///
/// Returns None if pricing is not available for the model.
/// Prices are in USD per 1M tokens (as of January 2025).
pub fn get_model_pricing(provider: &str, model: &str) -> Option<ModelPricing> {
    match (provider.to_lowercase().as_str(), model.to_lowercase().as_str()) {
        // OpenAI GPT-4o models
        ("openai", m) if m.contains("gpt-4o") && !m.contains("mini") => Some(ModelPricing {
            input_per_1m: 2.50,
            output_per_1m: 10.00,
        }),
        ("openai", m) if m.contains("gpt-4o-mini") => Some(ModelPricing {
            input_per_1m: 0.150,
            output_per_1m: 0.600,
        }),

        // OpenAI GPT-4 Turbo
        ("openai", m) if m.contains("gpt-4-turbo") || m == "gpt-4-1106-preview" || m == "gpt-4-0125-preview" => Some(ModelPricing {
            input_per_1m: 10.00,
            output_per_1m: 30.00,
        }),

        // OpenAI GPT-4
        ("openai", m) if m.starts_with("gpt-4") && !m.contains("turbo") && !m.contains("o") => Some(ModelPricing {
            input_per_1m: 30.00,
            output_per_1m: 60.00,
        }),

        // OpenAI GPT-3.5 Turbo
        ("openai", m) if m.contains("gpt-3.5-turbo") => Some(ModelPricing {
            input_per_1m: 0.50,
            output_per_1m: 1.50,
        }),

        // OpenAI o1 models (reasoning models)
        ("openai", m) if m.contains("o1-preview") => Some(ModelPricing {
            input_per_1m: 15.00,
            output_per_1m: 60.00,
        }),
        ("openai", m) if m.contains("o1-mini") => Some(ModelPricing {
            input_per_1m: 3.00,
            output_per_1m: 12.00,
        }),
        ("openai", m) if m == "o1" => Some(ModelPricing {
            input_per_1m: 15.00,
            output_per_1m: 60.00,
        }),

        // Anthropic Claude 3.5 Sonnet
        ("anthropic", m) if m.contains("claude-3-5-sonnet") || m.contains("claude-sonnet-4") => Some(ModelPricing {
            input_per_1m: 3.00,
            output_per_1m: 15.00,
        }),

        // Anthropic Claude 3.5 Haiku
        ("anthropic", m) if m.contains("claude-3-5-haiku") => Some(ModelPricing {
            input_per_1m: 0.80,
            output_per_1m: 4.00,
        }),

        // Anthropic Claude 3 Opus
        ("anthropic", m) if m.contains("claude-3-opus") => Some(ModelPricing {
            input_per_1m: 15.00,
            output_per_1m: 75.00,
        }),

        // Anthropic Claude 3 Sonnet
        ("anthropic", m) if m.contains("claude-3-sonnet") && !m.contains("3-5") => Some(ModelPricing {
            input_per_1m: 3.00,
            output_per_1m: 15.00,
        }),

        // Anthropic Claude 3 Haiku
        ("anthropic", m) if m.contains("claude-3-haiku") && !m.contains("3-5") => Some(ModelPricing {
            input_per_1m: 0.25,
            output_per_1m: 1.25,
        }),

        // Groq models (often free or very cheap, but we'll use placeholder pricing)
        ("groq", m) if m.contains("llama") => Some(ModelPricing {
            input_per_1m: 0.10,
            output_per_1m: 0.10,
        }),
        ("groq", m) if m.contains("mixtral") => Some(ModelPricing {
            input_per_1m: 0.24,
            output_per_1m: 0.24,
        }),

        // Unknown model - return None
        _ => None,
    }
}

/// Calculate cost in USD for an LLM API call
///
/// # Arguments
/// * `provider` - Provider name (e.g., "openai", "anthropic")
/// * `model` - Model name
/// * `input_tokens` - Number of input/prompt tokens
/// * `output_tokens` - Number of output/completion tokens
/// * `cached_tokens` - Optional cached input tokens (typically 90% discount)
///
/// # Returns
/// * `Some(cost)` - USD cost for the API call
/// * `None` - If pricing is not available for this model
pub fn calculate_cost(
    provider: &str,
    model: &str,
    input_tokens: u32,
    output_tokens: u32,
    cached_tokens: Option<u32>,
) -> Option<f64> {
    let pricing = get_model_pricing(provider, model)?;

    // Calculate input cost
    let input_cost = (input_tokens as f64 / 1_000_000.0) * pricing.input_per_1m;

    // Calculate output cost
    let output_cost = (output_tokens as f64 / 1_000_000.0) * pricing.output_per_1m;

    // Calculate cached token cost (typically 90% discount)
    let cached_cost = if let Some(cached) = cached_tokens {
        (cached as f64 / 1_000_000.0) * pricing.input_per_1m * 0.1
    } else {
        0.0
    };

    Some(input_cost + output_cost + cached_cost)
}

/// Set cost attributes on the span
///
/// Adds `gen_ai.usage.cost` (USD) and `gen_ai.usage.cost.currency` attributes.
pub fn set_cost_attributes(span: &mut impl Span, cost: f64) {
    span.set_attribute(KeyValue::new(attributes::USAGE_COST, cost));
    span.set_attribute(KeyValue::new(attributes::USAGE_COST_CURRENCY, "USD"));
}

/// Create a Gen AI span with proper naming and attributes
///
/// Creates a span named `chat {model}` with SpanKind::Client per OpenTelemetry spec.
/// Sets required attributes: operation.name, provider.name, request.model
///
/// The span will be created as a child of the provided parent context, or the current
/// context if none is provided, ensuring LLM calls appear in the same distributed trace
/// as the calling function.
///
/// # Arguments
/// * `provider` - The LLM provider name (e.g., "openai", "anthropic")
/// * `model` - The model name (e.g., "gpt-4", "claude-3-opus")
/// * `parent_context` - Optional parent context for trace propagation across async boundaries
pub fn create_gen_ai_span(
    provider: &str,
    model: &str,
    parent_context: Option<opentelemetry::Context>,
) -> impl Span {
    let tracer = global::tracer("agnt5-sdk-core");

    // Span name format: "{operation_name} {model_name}" per spec
    let span_name = format!("chat {}", model);

    // Use provided context or get current context to make this span a child of the calling function
    let ctx = parent_context.unwrap_or_else(opentelemetry::Context::current);

    let mut span = tracer
        .span_builder(span_name)
        .with_kind(SpanKind::Client)
        .start_with_context(&tracer, &ctx);

    // Required attributes per OpenTelemetry Gen AI conventions
    span.set_attribute(KeyValue::new(attributes::OPERATION_NAME, "chat"));
    span.set_attribute(KeyValue::new(attributes::PROVIDER_NAME, provider.to_string()));
    span.set_attribute(KeyValue::new(attributes::REQUEST_MODEL, model.to_string()));

    // Add tenant_id and deployment_id from global config
    if let Some(tid) = crate::telemetry::get_tenant_id() {
        span.set_attribute(KeyValue::new("tenant.id", tid.to_string()));
    }
    if let Some(did) = crate::telemetry::get_deployment_id() {
        span.set_attribute(KeyValue::new("deployment.id", did.to_string()));
    }

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
///     "role": "user",
///     "parts": [{"type": "text", "content": "Hello"}]
///   },
///   {
///     "role": "assistant",
///     "parts": [{"type": "text", "content": "Hi there!"}]
///   }
/// ]
/// ```
///
/// NOTE: System instructions are NOT included here - they should be captured
/// separately via `serialize_system_instructions()` in the `gen_ai.system_instructions` attribute.
pub fn serialize_input_messages(request: &GenerateRequest) -> Value {
    let mut messages_array = Vec::new();

    // Add conversation messages only (system instructions are separate)
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

/// Serialize system instructions to OpenTelemetry Gen AI format
///
/// Format per spec:
/// ```json
/// [
///   {
///     "type": "text",
///     "content": "You are a helpful assistant."
///   }
/// ]
/// ```
///
/// System instructions are provided to the model separately from the chat history
/// and should be captured in the `gen_ai.system_instructions` attribute.
pub fn serialize_system_instructions(system_prompt: &str) -> Value {
    json!([
        {
            "type": "text",
            "content": truncate_content(system_prompt, 10_000)
        }
    ])
}

/// Serialize output messages to OpenTelemetry Gen AI format
pub fn serialize_output_messages(response: &GenerateResponse) -> Value {
    let mut parts = Vec::new();

    // Add text content if present
    if !response.text.is_empty() {
        parts.push(json!({
            "type": "text",
            "content": &response.text
        }));
    }

    // Add tool calls if present
    if let Some(tool_calls) = &response.tool_calls {
        for tool_call in tool_calls {
            parts.push(json!({
                "type": "tool_call",
                "id": tool_call.id,
                "name": tool_call.name,
                "arguments": tool_call.arguments
            }));
        }
    }

    // If no parts, add empty text part for consistency
    if parts.is_empty() {
        parts.push(json!({
            "type": "text",
            "content": ""
        }));
    }

    json!([
        {
            "role": "assistant",
            "parts": parts
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

/// Serialize tool definitions to OpenTelemetry format
///
/// Format per spec (array of tool definitions):
/// ```json
/// [
///   {
///     "name": "search_web",
///     "description": "Search the web for information",
///     "parameters": {"type": "object", "properties": {...}}
///   }
/// ]
/// ```
pub fn serialize_tool_definitions(tools: &[ToolDefinition]) -> Value {
    let tools_array: Vec<Value> = tools
        .iter()
        .map(|tool| {
            let mut tool_obj = json!({
                "name": &tool.name,
            });

            if let Some(description) = &tool.description {
                tool_obj["description"] = json!(truncate_content(description, 500));
            }

            if let Some(parameters) = &tool.parameters {
                // Truncate parameter schema to prevent huge attributes
                let params_str = parameters.to_string();
                tool_obj["parameters"] = if params_str.len() > 2000 {
                    json!(format!("{}... [truncated]", &params_str[..2000]))
                } else {
                    parameters.clone()
                };
            }

            tool_obj
        })
        .collect();

    json!(tools_array)
}

/// Set tool-related request attributes on the span
pub fn set_tool_request_attributes(span: &mut impl Span, request: &GenerateRequest, capture_content: bool) {
    if !request.tools.is_empty() {
        // Always capture tool count
        span.set_attribute(KeyValue::new(
            "gen_ai.request.tools_count",
            request.tools.len() as i64,
        ));

        // Capture full tool definitions if content capture is enabled
        if capture_content {
            let tools_json = serialize_tool_definitions(&request.tools);
            span.set_attribute(KeyValue::new(
                "gen_ai.request.tools",
                tools_json.to_string(),
            ));
        }
    }

    // Capture tool choice if specified
    if let Some(tool_choice) = &request.tool_choice {
        let choice_str = format!("{:?}", tool_choice);
        span.set_attribute(KeyValue::new(
            "gen_ai.request.tool_choice",
            choice_str,
        ));
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
