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

use super::interface::{
    GenerateRequest, GenerateResponse, MessageRole, TokenUsage, ToolDefinition,
};

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
    pub const USAGE_CACHE_READ_TOKENS: &str = "gen_ai.usage.cache_read_input_tokens";
    pub const USAGE_CACHE_CREATION_TOKENS: &str = "gen_ai.usage.cache_creation_input_tokens";
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
    match (
        provider.to_lowercase().as_str(),
        model.to_lowercase().as_str(),
    ) {
        // ============================================================
        // OpenAI Models
        // ============================================================

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
        ("openai", m)
            if m.contains("gpt-4-turbo")
                || m == "gpt-4-1106-preview"
                || m == "gpt-4-0125-preview" =>
        {
            Some(ModelPricing {
                input_per_1m: 10.00,
                output_per_1m: 30.00,
            })
        }

        // OpenAI GPT-4
        ("openai", m) if m.starts_with("gpt-4") && !m.contains("turbo") && !m.contains("o") => {
            Some(ModelPricing {
                input_per_1m: 30.00,
                output_per_1m: 60.00,
            })
        }

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
        // OpenAI o3 models
        ("openai", m) if m.contains("o3-mini") => Some(ModelPricing {
            input_per_1m: 1.10,
            output_per_1m: 4.40,
        }),
        ("openai", m) if m == "o3" => Some(ModelPricing {
            input_per_1m: 10.00,
            output_per_1m: 40.00,
        }),

        // ============================================================
        // Anthropic Models
        // ============================================================

        // Anthropic Claude 4 Opus
        ("anthropic", m) if m.contains("claude-opus-4") || m.contains("claude-4-opus") => {
            Some(ModelPricing {
                input_per_1m: 15.00,
                output_per_1m: 75.00,
            })
        }

        // Anthropic Claude 4 Sonnet
        ("anthropic", m) if m.contains("claude-sonnet-4") || m.contains("claude-4-sonnet") => {
            Some(ModelPricing {
                input_per_1m: 3.00,
                output_per_1m: 15.00,
            })
        }

        // Anthropic Claude 3.5 Sonnet
        ("anthropic", m) if m.contains("claude-3-5-sonnet") => Some(ModelPricing {
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
        ("anthropic", m) if m.contains("claude-3-sonnet") && !m.contains("3-5") => {
            Some(ModelPricing {
                input_per_1m: 3.00,
                output_per_1m: 15.00,
            })
        }

        // Anthropic Claude 3 Haiku
        ("anthropic", m) if m.contains("claude-3-haiku") && !m.contains("3-5") => {
            Some(ModelPricing {
                input_per_1m: 0.25,
                output_per_1m: 1.25,
            })
        }

        // ============================================================
        // Google Gemini Models
        // ============================================================

        // Gemini 2.0 Flash
        ("google", m) if m.contains("gemini-2.0-flash") => Some(ModelPricing {
            input_per_1m: 0.10,
            output_per_1m: 0.40,
        }),

        // Gemini 1.5 Pro
        ("google", m) if m.contains("gemini-1.5-pro") => Some(ModelPricing {
            input_per_1m: 1.25,
            output_per_1m: 5.00,
        }),

        // Gemini 1.5 Flash
        ("google", m) if m.contains("gemini-1.5-flash") => Some(ModelPricing {
            input_per_1m: 0.075,
            output_per_1m: 0.30,
        }),

        // Gemini 1.0 Pro
        ("google", m) if m.contains("gemini-1.0-pro") || m.contains("gemini-pro") => {
            Some(ModelPricing {
                input_per_1m: 0.50,
                output_per_1m: 1.50,
            })
        }

        // ============================================================
        // DeepSeek Models
        // ============================================================

        // DeepSeek V3 (exceptionally cost-effective)
        ("deepseek", m) if m.contains("deepseek-chat") || m.contains("deepseek-v3") => {
            Some(ModelPricing {
                input_per_1m: 0.27,
                output_per_1m: 1.10,
            })
        }

        // DeepSeek R1 (reasoning model)
        ("deepseek", m) if m.contains("deepseek-reasoner") || m.contains("deepseek-r1") => {
            Some(ModelPricing {
                input_per_1m: 0.55,
                output_per_1m: 2.19,
            })
        }

        // DeepSeek Coder
        ("deepseek", m) if m.contains("deepseek-coder") => Some(ModelPricing {
            input_per_1m: 0.14,
            output_per_1m: 0.28,
        }),

        // ============================================================
        // xAI (Grok) Models
        // ============================================================

        // Grok-2
        ("xai", m) if m.contains("grok-2") && !m.contains("mini") => Some(ModelPricing {
            input_per_1m: 2.00,
            output_per_1m: 10.00,
        }),

        // Grok-2 Mini
        ("xai", m) if m.contains("grok-2-mini") => Some(ModelPricing {
            input_per_1m: 0.20,
            output_per_1m: 1.00,
        }),

        // Grok-beta
        ("xai", m) if m.contains("grok-beta") => Some(ModelPricing {
            input_per_1m: 5.00,
            output_per_1m: 15.00,
        }),

        // ============================================================
        // Mistral Models
        // ============================================================

        // Mistral Large
        ("mistral", m) if m.contains("mistral-large") => Some(ModelPricing {
            input_per_1m: 2.00,
            output_per_1m: 6.00,
        }),

        // Mistral Medium
        ("mistral", m) if m.contains("mistral-medium") => Some(ModelPricing {
            input_per_1m: 2.70,
            output_per_1m: 8.10,
        }),

        // Mistral Small
        ("mistral", m) if m.contains("mistral-small") => Some(ModelPricing {
            input_per_1m: 0.20,
            output_per_1m: 0.60,
        }),

        // Codestral
        ("mistral", m) if m.contains("codestral") => Some(ModelPricing {
            input_per_1m: 0.20,
            output_per_1m: 0.60,
        }),

        // Ministral 8B
        ("mistral", m) if m.contains("ministral-8b") => Some(ModelPricing {
            input_per_1m: 0.10,
            output_per_1m: 0.10,
        }),

        // Ministral 3B
        ("mistral", m) if m.contains("ministral-3b") => Some(ModelPricing {
            input_per_1m: 0.04,
            output_per_1m: 0.04,
        }),

        // Pixtral Large
        ("mistral", m) if m.contains("pixtral-large") => Some(ModelPricing {
            input_per_1m: 2.00,
            output_per_1m: 6.00,
        }),

        // Pixtral 12B
        ("mistral", m) if m.contains("pixtral-12b") => Some(ModelPricing {
            input_per_1m: 0.15,
            output_per_1m: 0.15,
        }),

        // Open Mixtral 8x22B
        ("mistral", m) if m.contains("mixtral-8x22b") || m.contains("open-mixtral-8x22b") => {
            Some(ModelPricing {
                input_per_1m: 2.00,
                output_per_1m: 6.00,
            })
        }

        // Open Mixtral 8x7B
        ("mistral", m) if m.contains("mixtral-8x7b") || m.contains("open-mixtral-8x7b") => {
            Some(ModelPricing {
                input_per_1m: 0.70,
                output_per_1m: 0.70,
            })
        }

        // Open Mistral 7B
        ("mistral", m) if m.contains("open-mistral-7b") || m.contains("mistral-7b") => {
            Some(ModelPricing {
                input_per_1m: 0.25,
                output_per_1m: 0.25,
            })
        }

        // ============================================================
        // Groq Models (hosted inference)
        // ============================================================

        // Llama 3.3 70B
        ("groq", m) if m.contains("llama-3.3-70b") => Some(ModelPricing {
            input_per_1m: 0.59,
            output_per_1m: 0.79,
        }),

        // Llama 3.2 90B Vision
        ("groq", m) if m.contains("llama-3.2-90b") => Some(ModelPricing {
            input_per_1m: 0.90,
            output_per_1m: 0.90,
        }),

        // Llama 3.2 11B Vision
        ("groq", m) if m.contains("llama-3.2-11b") => Some(ModelPricing {
            input_per_1m: 0.18,
            output_per_1m: 0.18,
        }),

        // Llama 3.1 70B
        ("groq", m) if m.contains("llama-3.1-70b") => Some(ModelPricing {
            input_per_1m: 0.59,
            output_per_1m: 0.79,
        }),

        // Llama 3.1 8B
        ("groq", m) if m.contains("llama-3.1-8b") => Some(ModelPricing {
            input_per_1m: 0.05,
            output_per_1m: 0.08,
        }),

        // Llama 3 70B
        ("groq", m) if m.contains("llama3-70b") || m.contains("llama-3-70b") => {
            Some(ModelPricing {
                input_per_1m: 0.59,
                output_per_1m: 0.79,
            })
        }

        // Llama 3 8B
        ("groq", m) if m.contains("llama3-8b") || m.contains("llama-3-8b") => Some(ModelPricing {
            input_per_1m: 0.05,
            output_per_1m: 0.08,
        }),

        // Generic Llama models
        ("groq", m) if m.contains("llama") => Some(ModelPricing {
            input_per_1m: 0.10,
            output_per_1m: 0.10,
        }),

        // Mixtral 8x7B
        ("groq", m) if m.contains("mixtral") => Some(ModelPricing {
            input_per_1m: 0.24,
            output_per_1m: 0.24,
        }),

        // Gemma 2 9B
        ("groq", m) if m.contains("gemma2-9b") => Some(ModelPricing {
            input_per_1m: 0.20,
            output_per_1m: 0.20,
        }),

        // Gemma 7B
        ("groq", m) if m.contains("gemma-7b") => Some(ModelPricing {
            input_per_1m: 0.07,
            output_per_1m: 0.07,
        }),

        // ============================================================
        // Ollama Models (local - no cost)
        // ============================================================

        // Ollama models run locally, so cost is $0
        ("ollama", _) => Some(ModelPricing {
            input_per_1m: 0.0,
            output_per_1m: 0.0,
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
/// * `cached_tokens` - Optional cache-hit input tokens. Must be a subset of
///   `input_tokens` (the OpenAI usage convention). These are billed at a steep
///   discount (~10% of the input rate).
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

    // Cache-hit tokens are part of `input_tokens` but billed at a discount, so
    // split the input into the cached portion and the full-price remainder.
    // Clamp to `input_tokens` to stay robust against inconsistent provider usage
    // objects.
    let cached = cached_tokens.unwrap_or(0).min(input_tokens);
    let non_cached_input = input_tokens - cached;

    let input_cost = (non_cached_input as f64 / 1_000_000.0) * pricing.input_per_1m;
    let cached_cost = (cached as f64 / 1_000_000.0) * pricing.input_per_1m * 0.1;
    let output_cost = (output_tokens as f64 / 1_000_000.0) * pricing.output_per_1m;

    Some(input_cost + cached_cost + output_cost)
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
    span.set_attribute(KeyValue::new(
        attributes::PROVIDER_NAME,
        provider.to_string(),
    ));
    span.set_attribute(KeyValue::new(attributes::REQUEST_MODEL, model.to_string()));

    if let Some(pid) = crate::telemetry::get_project_id() {
        span.set_attribute(KeyValue::new("agnt5.project.id", pid.to_string()));
    }
    if let Some(wid) = crate::telemetry::get_workspace_id() {
        span.set_attribute(KeyValue::new("agnt5.workspace.id", wid.to_string()));
    }
    if let Some(did) = crate::telemetry::get_deployment_id() {
        span.set_attribute(KeyValue::new("agnt5.deployment.id", did.to_string()));
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
        span.set_attribute(KeyValue::new(attributes::REQUEST_TOP_P, top_p as f64));
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

    if let Some(cached_tokens) = usage.cached_tokens {
        span.set_attribute(KeyValue::new(
            attributes::USAGE_CACHE_READ_TOKENS,
            cached_tokens as i64,
        ));
    }

    if let Some(cache_creation_tokens) = usage.cache_creation_tokens {
        span.set_attribute(KeyValue::new(
            attributes::USAGE_CACHE_CREATION_TOKENS,
            cache_creation_tokens as i64,
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
pub fn set_tool_request_attributes(
    span: &mut impl Span,
    request: &GenerateRequest,
    capture_content: bool,
) {
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
        span.set_attribute(KeyValue::new("gen_ai.request.tool_choice", choice_str));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_capture_content_default_true() {
        // Content capture is enabled by default (see docstring on
        // `should_capture_content`). Setting `AGNT5_LLM_CAPTURE_CONTENT=false`
        // or `=0` is the opt-out path.
        //
        // This test assumes the env var is unset; if another test sets it
        // and doesn't clean up, this test becomes order-dependent. Run the
        // tests with `--test-threads=1` if that happens.
        std::env::remove_var("AGNT5_LLM_CAPTURE_CONTENT");
        assert!(should_capture_content());
    }

    #[test]
    fn test_calculate_cost_discounts_cached_tokens() {
        // gpt-4o input is $2.50 / 1M tokens. Use 0 output tokens to isolate the
        // input-side math. With 1000 input tokens and no cache, the full input
        // price applies.
        let full = calculate_cost("openai", "gpt-4o", 1000, 0, None).unwrap();
        assert!((full - 0.0025).abs() < 1e-9, "full cost was {full}");

        // With 500 of those 1000 tokens served from cache, the cached half is
        // billed at 10% and the rest at full price:
        //   500/1e6 * 2.50      = 0.00125
        //   500/1e6 * 2.50 * 0.1 = 0.000125
        let cached = calculate_cost("openai", "gpt-4o", 1000, 0, Some(500)).unwrap();
        assert!((cached - 0.001375).abs() < 1e-9, "cached cost was {cached}");
        assert!(cached < full, "caching should never increase cost");
    }

    #[test]
    fn test_calculate_cost_clamps_cached_over_input() {
        // A malformed usage object reporting more cached tokens than input
        // tokens must not produce a negative or inflated cost.
        let cost = calculate_cost("openai", "gpt-4o", 1000, 0, Some(5000)).unwrap();
        // All 1000 input tokens treated as cached: 1000/1e6 * 2.50 * 0.1.
        assert!((cost - 0.00025).abs() < 1e-9, "clamped cost was {cost}");
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
    fn test_serialize_input_messages_excludes_system_prompt() {
        // Per the docstring on `serialize_input_messages`, system
        // instructions are NOT included — they're serialized separately
        // via `serialize_system_instructions()` and live in the
        // `gen_ai.system_instructions` attribute, not in `input.messages`.
        // So the returned array should only contain the conversation
        // messages (the user turn in this case).
        let request = GenerateRequest::new("gpt-4")
            .system_prompt("You are helpful")
            .user_message("Hello");

        let serialized = serialize_input_messages(&request);
        assert!(serialized.is_array());

        let array = serialized.as_array().unwrap();
        assert_eq!(array.len(), 1);
        assert_eq!(array[0]["role"], "user");

        // The system prompt is available through the separate serializer.
        let system = serialize_system_instructions("You are helpful");
        assert!(system.is_array());
        assert_eq!(system.as_array().unwrap().len(), 1);
        assert_eq!(system[0]["content"], "You are helpful");
    }
}
