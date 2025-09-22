// LLM telemetry integration with AGNT5's OpenTelemetry infrastructure
use super::models::{
    ChatCompletion, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse,
    ChatMessageContent, CompletionRequest, CompletionResponse, EmbeddingUsage, EmbeddingsInput,
    EmbeddingsRequest, EmbeddingsResponse, Usage,
};
use super::provider::{get_vendor_name, ProviderType};
use crate::telemetry::{create_function_span, record_span_error, record_span_success};
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::trace::Span;
use opentelemetry::{global, KeyValue};
use opentelemetry_semantic_conventions::trace::*;
use std::sync::OnceLock;
use std::time::Instant;

/// Trait for recording span attributes from LLM requests/responses
pub trait RecordSpan {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan);
}

struct LlmMetrics {
    request_counter: Counter<u64>,
    latency_histogram: Histogram<f64>,
    tokens_counter: Counter<u64>,
}

impl LlmMetrics {
    fn new() -> Self {
        let meter = global::meter("agnt5-sdk-core.llm");
        let request_counter = meter
            .u64_counter("llm.request.count")
            .with_description("Count of LLM proxy requests")
            .build();
        let latency_histogram = meter
            .f64_histogram("llm.latency.ms")
            .with_description("LLM request latency in milliseconds")
            .build();
        let tokens_counter = meter
            .u64_counter("llm.tokens.total")
            .with_description("Total tokens consumed by LLM proxy")
            .build();
        Self {
            request_counter,
            latency_histogram,
            tokens_counter,
        }
    }

    fn record(&self, mode: &str, model: &str, status: &str, latency_ms: f64, total_tokens: u64) {
        let attributes = [
            KeyValue::new("mode", mode.to_string()),
            KeyValue::new("model", model.to_string()),
            KeyValue::new("status", status.to_string()),
        ];
        self.request_counter.add(1, &attributes);
        self.latency_histogram.record(latency_ms, &attributes);
        if total_tokens > 0 {
            self.tokens_counter.add(total_tokens, &attributes);
        }
    }
}

fn metrics() -> &'static LlmMetrics {
    static METRICS: OnceLock<LlmMetrics> = OnceLock::new();
    METRICS.get_or_init(LlmMetrics::new)
}

/// LLM-specific span wrapper that integrates with AGNT5's telemetry
pub struct LlmSpan {
    span: opentelemetry::global::BoxedSpan,
    accumulated_completion: Option<ChatCompletion>,
    start: Instant,
    mode: &'static str,
    model: Option<String>,
    latest_usage: Option<Usage>,
    metrics_recorded: bool,
}

impl LlmSpan {
    /// Start a new span for chat completion
    pub fn start_chat_completion(
        request: &ChatCompletionRequest,
        provider_type: ProviderType,
    ) -> Self {
        let span = create_function_span(
            "llm.chat_completion",
            "llm-service",
            "llm-worker",
            "llm-invocation",
            None,
            None,
        );
        let mut llm_span = Self {
            span,
            accumulated_completion: None,
            start: Instant::now(),
            mode: "chat",
            model: Some(request.model.clone()),
            latest_usage: None,
            metrics_recorded: false,
        };

        // Record request attributes
        llm_span.set_vendor(&provider_type);
        request.record_span(&mut llm_span.span);

        llm_span
    }

    /// Start a new span for completion
    pub fn start_completion(request: &CompletionRequest, provider_type: ProviderType) -> Self {
        let span = create_function_span(
            "llm.completion",
            "llm-service",
            "llm-worker",
            "llm-invocation",
            None,
            None,
        );
        let mut llm_span = Self {
            span,
            accumulated_completion: None,
            start: Instant::now(),
            mode: "complete",
            model: Some(request.model.clone()),
            latest_usage: None,
            metrics_recorded: false,
        };

        llm_span.set_vendor(&provider_type);
        request.record_span(&mut llm_span.span);

        llm_span
    }

    /// Start a new span for embeddings
    pub fn start_embeddings(request: &EmbeddingsRequest, provider_type: ProviderType) -> Self {
        let span = create_function_span(
            "llm.embeddings",
            "llm-service",
            "llm-worker",
            "llm-invocation",
            None,
            None,
        );
        let mut llm_span = Self {
            span,
            accumulated_completion: None,
            start: Instant::now(),
            mode: "embeddings",
            model: Some(request.model.clone()),
            latest_usage: None,
            metrics_recorded: false,
        };

        llm_span.set_vendor(&provider_type);
        request.record_span(&mut llm_span.span);

        llm_span
    }

    /// Set the vendor attribute for the span
    pub fn set_vendor(&mut self, provider_type: &ProviderType) {
        let vendor = get_vendor_name(provider_type);
        self.span
            .set_attribute(KeyValue::new("gen_ai.system", vendor.into_owned()));
    }

    /// Log a streaming chunk
    pub fn log_chunk(&mut self, chunk: &ChatCompletionChunk) {
        if self.accumulated_completion.is_none() {
            self.accumulated_completion = Some(ChatCompletion {
                id: chunk.id.clone(),
                object: None,
                created: None,
                model: chunk.model.clone(),
                choices: vec![],
                usage: chunk.usage.clone().unwrap_or_default(),
                system_fingerprint: chunk.system_fingerprint.clone(),
            });
        }

        // Accumulate chunk content
        if let Some(completion) = &mut self.accumulated_completion {
            for chunk_choice in &chunk.choices {
                if let Some(existing_choice) =
                    completion.choices.get_mut(chunk_choice.index as usize)
                {
                    if let Some(content) = &chunk_choice.delta.content {
                        if let Some(ChatMessageContent::String(existing_content)) =
                            &mut existing_choice.message.content
                        {
                            existing_content.push_str(content);
                        }
                    }
                    if chunk_choice.finish_reason.is_some() {
                        existing_choice.finish_reason = chunk_choice.finish_reason.clone();
                    }
                } else {
                    use super::models::{ChatChoice, ChatMessage};
                    completion.choices.push(ChatChoice {
                        index: chunk_choice.index,
                        message: ChatMessage {
                            role: chunk_choice
                                .delta
                                .role
                                .clone()
                                .unwrap_or_else(|| "assistant".to_string()),
                            content: Some(ChatMessageContent::String(
                                chunk_choice.delta.content.clone().unwrap_or_default(),
                            )),
                            name: None,
                            tool_calls: chunk_choice.delta.tool_calls.clone(),
                            tool_call_id: None,
                            refusal: chunk_choice.delta.refusal.clone(),
                        },
                        finish_reason: chunk_choice.finish_reason.clone(),
                        logprobs: chunk_choice.logprobs.clone(),
                    });
                }
            }

            // Update usage if present
            if let Some(usage) = &chunk.usage {
                completion.usage = usage.clone();
                self.latest_usage = Some(usage.clone());
            }
        }
    }

    /// Log successful chat completion response
    pub fn log_chat_success(&mut self, response: &ChatCompletionResponse) {
        response.record_span(&mut self.span);
        record_span_success(&mut self.span, 0);

        match response {
            ChatCompletionResponse::NonStream(completion) => {
                self.model = Some(completion.model.clone());
                self.latest_usage = Some(completion.usage.clone());
            }
            ChatCompletionResponse::Stream(_) => {
                // usage recorded via streaming chunks
            }
        }

        self.record_metrics("success");
    }

    /// Log successful completion response
    pub fn log_completion_success(&mut self, response: &CompletionResponse) {
        response.record_span(&mut self.span);
        record_span_success(&mut self.span, 0);
        self.model = Some(response.model.clone());
        self.latest_usage = Some(response.usage.clone());
        self.record_metrics("success");
    }

    /// Log successful embeddings response
    pub fn log_embeddings_success(&mut self, response: &EmbeddingsResponse) {
        response.record_span(&mut self.span);
        record_span_success(&mut self.span, 0);
        self.model = Some(response.model.clone());
        self.record_metrics("success");
    }

    /// Log error
    pub fn log_error(&mut self, error: &crate::error::SdkError) {
        record_span_error(&mut self.span, &error.to_string());
        self.record_metrics("error");
    }

    /// Finish the span (called automatically on drop)
    pub fn finish(mut self) {
        // If we have accumulated completion from streaming, record it
        if let Some(completion) = &self.accumulated_completion {
            completion.record_span(&mut self.span);
            self.latest_usage = Some(completion.usage.clone());
        }
        self.record_metrics("success");
    }

    fn record_metrics(&mut self, status: &str) {
        if self.metrics_recorded {
            return;
        }
        let latency_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        let model = self.model.clone().unwrap_or_else(|| "unknown".to_string());
        let total_tokens = self
            .latest_usage
            .as_ref()
            .map(|usage| usage.prompt_tokens as u64 + usage.completion_tokens as u64)
            .unwrap_or(0);
        metrics().record(self.mode, &model, status, latency_ms, total_tokens);
        self.metrics_recorded = true;
    }
}

impl Drop for LlmSpan {
    fn drop(&mut self) {
        // Record accumulated completion if we have one
        if let Some(completion) = &self.accumulated_completion {
            completion.record_span(&mut self.span);
            self.latest_usage = Some(completion.usage.clone());
        }
        self.record_metrics("success");
    }
}

// Implement RecordSpan for request types
impl RecordSpan for ChatCompletionRequest {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new("llm.request.type", "chat"));
        span.set_attribute(KeyValue::new(GEN_AI_REQUEST_MODEL, self.model.clone()));

        if let Some(max_tokens) = self.max_tokens.or(self.max_completion_tokens) {
            span.set_attribute(KeyValue::new(GEN_AI_REQUEST_MAX_TOKENS, max_tokens as i64));
        }

        if let Some(freq_penalty) = self.frequency_penalty {
            span.set_attribute(KeyValue::new(
                GEN_AI_REQUEST_FREQUENCY_PENALTY,
                freq_penalty as f64,
            ));
        }
        if let Some(pres_penalty) = self.presence_penalty {
            span.set_attribute(KeyValue::new(
                GEN_AI_REQUEST_PRESENCE_PENALTY,
                pres_penalty as f64,
            ));
        }
        if let Some(top_p) = self.top_p {
            span.set_attribute(KeyValue::new(GEN_AI_REQUEST_TOP_P, top_p as f64));
        }
        if let Some(temp) = self.temperature {
            span.set_attribute(KeyValue::new(GEN_AI_REQUEST_TEMPERATURE, temp as f64));
        }

        // Record message content if trace content is enabled
        if should_trace_content() {
            for (i, message) in self.messages.iter().enumerate() {
                span.set_attribute(KeyValue::new(
                    format!("gen_ai.prompt.{}.role", i),
                    message.role.clone(),
                ));
                if let Some(content) = &message.content {
                    span.set_attribute(KeyValue::new(
                        format!("gen_ai.prompt.{}.content", i),
                        match content {
                            ChatMessageContent::String(content) => content.clone(),
                            ChatMessageContent::Array(content) => {
                                serde_json::to_string(&content).unwrap_or_default()
                            }
                        },
                    ));
                }
            }
        }
    }
}

impl RecordSpan for ChatCompletion {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new(GEN_AI_RESPONSE_MODEL, self.model.clone()));
        span.set_attribute(KeyValue::new(GEN_AI_RESPONSE_ID, self.id.clone()));

        self.usage.record_span(span);

        if should_trace_content() {
            for choice in &self.choices {
                if let Some(content) = &choice.message.content {
                    span.set_attribute(KeyValue::new(
                        format!("gen_ai.completion.{}.role", choice.index),
                        choice.message.role.clone(),
                    ));
                    span.set_attribute(KeyValue::new(
                        format!("gen_ai.completion.{}.content", choice.index),
                        match content {
                            ChatMessageContent::String(content) => content.clone(),
                            ChatMessageContent::Array(content) => {
                                serde_json::to_string(&content).unwrap_or_default()
                            }
                        },
                    ));
                }
                span.set_attribute(KeyValue::new(
                    format!("gen_ai.completion.{}.finish_reason", choice.index),
                    choice.finish_reason.clone().unwrap_or_default(),
                ));
            }
        }
    }
}

impl RecordSpan for ChatCompletionResponse {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        match self {
            ChatCompletionResponse::NonStream(completion) => {
                completion.record_span(span);
            }
            ChatCompletionResponse::Stream(_) => {
                // Stream responses are handled by accumulating chunks
                span.set_attribute(KeyValue::new("gen_ai.response.stream", true));
            }
        }
    }
}

impl RecordSpan for CompletionRequest {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new("llm.request.type", "completion"));
        span.set_attribute(KeyValue::new(GEN_AI_REQUEST_MODEL, self.model.clone()));

        if let Some(max_tokens) = self.max_tokens {
            span.set_attribute(KeyValue::new(GEN_AI_REQUEST_MAX_TOKENS, max_tokens as i64));
        }

        if let Some(freq_penalty) = self.frequency_penalty {
            span.set_attribute(KeyValue::new(
                GEN_AI_REQUEST_FREQUENCY_PENALTY,
                freq_penalty as f64,
            ));
        }
        if let Some(pres_penalty) = self.presence_penalty {
            span.set_attribute(KeyValue::new(
                GEN_AI_REQUEST_PRESENCE_PENALTY,
                pres_penalty as f64,
            ));
        }
        if let Some(top_p) = self.top_p {
            span.set_attribute(KeyValue::new(GEN_AI_REQUEST_TOP_P, top_p as f64));
        }
        if let Some(temp) = self.temperature {
            span.set_attribute(KeyValue::new(GEN_AI_REQUEST_TEMPERATURE, temp as f64));
        }

        if should_trace_content() {
            span.set_attribute(KeyValue::new("gen_ai.prompt", self.prompt.clone()));
        }
    }
}

impl RecordSpan for CompletionResponse {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new(GEN_AI_RESPONSE_MODEL, self.model.clone()));
        span.set_attribute(KeyValue::new(GEN_AI_RESPONSE_ID, self.id.clone()));

        self.usage.record_span(span);

        if should_trace_content() {
            for choice in &self.choices {
                span.set_attribute(KeyValue::new(
                    format!("gen_ai.completion.{}.content", choice.index),
                    choice.text.clone(),
                ));
                span.set_attribute(KeyValue::new(
                    format!("gen_ai.completion.{}.finish_reason", choice.index),
                    choice.finish_reason.clone().unwrap_or_default(),
                ));
            }
        }
    }
}

impl RecordSpan for EmbeddingsRequest {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new("llm.request.type", "embeddings"));
        span.set_attribute(KeyValue::new(GEN_AI_REQUEST_MODEL, self.model.clone()));

        let input_count = match &self.input {
            EmbeddingsInput::String(_) => 1,
            EmbeddingsInput::Array(arr) => arr.len(),
        };
        span.set_attribute(KeyValue::new(
            "gen_ai.embeddings.input_count",
            input_count as i64,
        ));

        if let Some(dimensions) = self.dimensions {
            span.set_attribute(KeyValue::new(
                "gen_ai.embeddings.dimensions",
                dimensions as i64,
            ));
        }
    }
}

impl RecordSpan for EmbeddingsResponse {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new(GEN_AI_RESPONSE_MODEL, self.model.clone()));
        span.set_attribute(KeyValue::new(
            "gen_ai.embeddings.vector_count",
            self.data.len() as i64,
        ));

        if let Some(dimension) = self.dimension() {
            span.set_attribute(KeyValue::new(
                "gen_ai.embeddings.dimension",
                dimension as i64,
            ));
        }

        self.usage.record_span(span);
    }
}

impl RecordSpan for Usage {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new(
            GEN_AI_USAGE_INPUT_TOKENS,
            self.prompt_tokens as i64,
        ));
        span.set_attribute(KeyValue::new(
            GEN_AI_USAGE_OUTPUT_TOKENS,
            self.completion_tokens as i64,
        ));

        // Add reasoning tokens if available
        if let Some(details) = &self.completion_tokens_details {
            if let Some(reasoning_tokens) = details.reasoning_tokens {
                span.set_attribute(KeyValue::new(
                    "gen_ai.usage.reasoning_tokens",
                    reasoning_tokens as i64,
                ));
            }
        }

        // Add cached tokens if available
        if let Some(details) = &self.prompt_tokens_details {
            if let Some(cached_tokens) = details.cached_tokens {
                span.set_attribute(KeyValue::new(
                    "gen_ai.usage.cached_tokens",
                    cached_tokens as i64,
                ));
            }
        }
    }
}

impl RecordSpan for EmbeddingUsage {
    fn record_span(&self, span: &mut opentelemetry::global::BoxedSpan) {
        span.set_attribute(KeyValue::new(
            GEN_AI_USAGE_INPUT_TOKENS,
            self.prompt_tokens as i64,
        ));
    }
}

/// Check if content tracing is enabled (environment variable)
fn should_trace_content() -> bool {
    std::env::var("AGNT5_TRACE_CONTENT_ENABLED")
        .unwrap_or_else(|_| "false".to_string())
        .parse()
        .unwrap_or(false)
}
