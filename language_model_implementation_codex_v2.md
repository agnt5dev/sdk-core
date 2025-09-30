# Rust Language Model SDK Implementation Plan

This document captures the end-to-end plan for delivering a Rust-first language model SDK with a single provider (OpenAI) and two high-level APIs: `generate` and `stream`. The goal is to deliver production-quality crates that follow idiomatic Rust semantics rather than porting the TypeScript AI SDK feature-for-feature.

> **Implementation note (2025-02)**: the first iteration now lives in `sdk-core/src/lm` as a small set of modules: `interface.rs` defines the provider-agnostic `LanguageModel` surface plus shared request/response types (`GenerateRequest` + `GenerationConfig`), while the provider modules (`openai.rs`, `anthropic.rs`, `azure.rs`, `groq.rs`, `openrouter.rs`, and `bedrock.rs`) embed the initial integrations. Model identifiers follow a `provider/model` convention (e.g., `openai/gpt-4o-mini`, `azure/my-deployment`, `openrouter/deepseek/deepseek-chat`, `groq/llama3-8b`, `bedrock/us-east-1/anthropic.claude-3-haiku-20240307`); each module strips the prefix before calling its REST endpoint. The broader multi-crate layout below remains aspirational until we expand beyond the embedded MVP.

## 1. Scope & Guiding Principles
- Support **non-streaming** (`generate`) and **streaming** (`stream`) calls against OpenAI-compatible language models.
- Expose clean, ergonomic Rust APIs that integrate with `tokio` / async ecosystems.
- Provide a provider-agnostic core so other integrations can be added later, but only ship the OpenAI provider for now.
- Embrace Rust norms: builders for configuration, strong typing, error enums, `futures` streams, `tracing` instrumentation.
- Avoid copying AI SDK internals; re-think APIs with Rust ergonomics in mind.

## 2. Project Layout
```
crates/
  core/
    Cargo.toml
    src/
      api/
        generate.rs
        stream.rs
        mod.rs
      model/
        traits.rs
        registry.rs
        resolve.rs
      prompt/
        message.rs
        normalize.rs
      telemetry/
        span.rs
        metrics.rs (optional)
      error.rs
      types.rs
      util/
        retry.rs
        http.rs
  provider-openai/
    Cargo.toml
    src/
      config.rs
      client.rs
      model.rs
      chunk.rs
      error.rs
      lib.rs
examples/
  generate_openai.rs
  stream_openai.rs
```

## 3. Core Crate Design
### 3.1 Public API Surface
```rust
pub struct GenerateOptions<'a> { /* builder */ }
pub struct GenerateResponse { /* text, usage, metadata */ }

pub struct StreamOptions<'a> { /* builder */ }
pub struct StreamHandle { /* stream + metadata futures */ }

pub async fn generate(options: GenerateOptions<'_>) -> Result<GenerateResponse, SdkError>;
pub async fn stream(options: StreamOptions<'_>) -> Result<StreamHandle, SdkError>;
```
- Options use a builder pattern (`GenerateOptions::new(model).system("...")`) with lifetimes to borrow prompt/message data when possible.
- `SdkError` is an enum covering validation errors, provider resolution failures, transport issues, etc.
- Responses expose provider metadata (request id, model id, issued at) & token usage.

### 3.2 Model Abstraction
```rust
#[async_trait]
pub trait LanguageModel: Send + Sync {
    fn provider_name(&self) -> &'static str;
    fn model_id(&self) -> &str;
    async fn generate(&self, request: ModelGenerateRequest) -> Result<ModelGenerateResponse, ModelError>;
    async fn stream(&self, request: ModelStreamRequest) -> Result<ModelStreamResponse, ModelError>;
}
```
- `ModelGenerateRequest` carries normalized messages, sampling settings, user parameters.
- `ModelStreamResponse` wraps a `Pin<Box<dyn Stream<Item = Result<ModelStreamChunk, ModelError>> + Send>>` plus response metadata.
- `ModelStreamChunk` is an enum (`DeltaText`, `Usage`, `Done`, `ProviderRaw`).

### 3.3 Model Resolution
- `ModelHandle` enum (`ById(String)`, `Direct(Arc<dyn LanguageModel>)`).
- `ProviderRegistry`: thread-safe (use `DashMap` or `RwLock<HashMap>`) mapping provider name → `Arc<dyn ProviderFactory>`.
- Global default: `OnceLock<ProviderRegistry>` with `set_default_provider` helper.
- `resolve_model(handle, registry)` returns `Arc<dyn LanguageModel>`.

### 3.4 Prompt Representation
- Define `Message { role: Role, content: Vec<MessagePart> }` with roles `System`, `User`, `Assistant`, `Tool`.
- `MessagePart` covers text blocks and optional data attachments (future extensibility).
- `PromptNormalizer` merges single `prompt` strings into user messages, ensures first system message is in place.

### 3.5 Retry & Telemetry
- `RetryPolicy` struct with max attempts, backoff, timeouts; default to exponential with jitter using `backoff` crate.
- Telemetry integration via `tracing`: wrap high-level API entry points in spans; propagate provider response ids.
- Provide optional feature `telemetry-opentelemetry` for OTLP exporters (out of scope for MVP but allow hooking in).

### 3.6 Error Types
- Core error enums:
  ```rust
  pub enum SdkError {
      Validation(ValidationError),
      Provider(ProviderError),
      Transport(TransportError),
      Model(ModelError),
      StreamClosed,
  }
  ```
- Implement `std::error::Error` + `Display` for each, with `thiserror` derive.

## 4. OpenAI Provider Crate
### 4.1 Configuration
```rust
pub struct OpenAiConfig {
    pub api_base: Url,
    pub api_key: SecretString,
    pub organization: Option<String>,
    pub project: Option<String>,
    pub timeout: Duration,
}

impl OpenAiConfig {
    pub fn from_env() -> Result<Self, ConfigError>;
}
```
- Use `secrecy::SecretString` for tokens.
- Provide `OpenAiProvider::new(config)` returning `Arc<OpenAiProvider>` implementing `LanguageModel`.

### 4.2 HTTP Client
- `reqwest::Client` with default headers and user-agent (`language-model-sdk/{version}`).
- Implement JSON serialization using `serde` for request/response payloads.
- Non-streaming: POST `/v1/responses` (or `/v1/chat/completions` if targeting stable).
- Streaming: same endpoint with `stream: true`, parse Server-Sent Events.

### 4.3 Model Implementation
```rust
pub struct OpenAiModel {
    client: Arc<Client>,
    config: Arc<OpenAiConfig>,
    model_id: String,
}

#[async_trait]
impl LanguageModel for OpenAiModel {
    async fn generate(&self, request: ModelGenerateRequest) -> Result<ModelGenerateResponse, ModelError> { /* ... */ }
    async fn stream(&self, request: ModelStreamRequest) -> Result<ModelStreamResponse, ModelError> { /* ... */ }
}
```
- Translate normalized messages to OpenAI payload (system/user roles).
- Map sampling params (`temperature`, `top_p`, `max_tokens`).
- Capture tokens usage from response if provided; fallback to `None`.
- For streaming, use `reqwest::Response::bytes_stream()` and parse SSE frames using `eventsource_stream` or manual splitting.

### 4.4 Stream Handling
- Emit `ModelStreamChunk::DeltaText { content }` for text deltas.
- When `logprobs`, `usage`, or final messages arrive, emit corresponding chunk variants.
- Add `ModelStreamChunk::ProviderRaw(Bytes)` for diagnostics if caller opts-in.

### 4.5 Provider Registration
```rust
pub struct OpenAiProviderFactory { /* holds Arc<OpenAiConfig> */ }

impl ProviderFactory for OpenAiProviderFactory {
    fn language_model(&self, id: &str) -> Result<Arc<dyn LanguageModel>, ProviderError> {
        Ok(Arc::new(OpenAiModel::new(self.config.clone(), id.to_owned())))
    }
}
```
- Register default provider in examples: `Registry::global().register("openai", OpenAiProviderFactory::new(config));`

## 5. High-Level API Behavior
### 5.1 `generate`
1. Validate options: ensure prompt/messages specified, sampling values within ranges.
2. Resolve model via registry.
3. Normalize messages (prepend system message if provided).
4. Construct `ModelGenerateRequest` with collected options (including optional metadata).
5. Wrap call in retry loop: on transient errors (HTTP 429/5xx, timeouts) obey policy.
6. Populate `GenerateResponse` with:
   - `text`: aggregated assistant message content.
   - `usage`: struct with optional tokens.
   - `model`: provider & id.
   - `raw_response`: optional JSON when debug flag set.

### 5.2 `stream`
1. Same setup as `generate` but call `model.stream` once (no retry after stream starts).
2. Wrap provider stream with `StreamTransformer` to:
   - Convert to public chunk enum (`StreamChunk::Text`, `StreamChunk::Error`, `StreamChunk::Metadata`).
   - Maintain running text buffer for `StreamHandle::join()` convenience method.
3. Expose `StreamHandle` API:
   ```rust
   impl StreamHandle {
       pub fn chunks(&self) -> impl Stream<Item = Result<StreamChunk, SdkError>>;
       pub async fn collect_text(self) -> Result<GenerateResponse, SdkError>;
   }
   ```
   `collect_text` waits for stream completion and returns same structure as `generate`.

## 6. Detailed Implementation Steps
1. **Bootstrap crates**: initialize `crates/core` and `crates/provider-openai` with workspace `Cargo.toml`.
2. **Define shared types**: enums for roles, chunk types, token usage; implement serde if needed for serialization in tests.
3. **Implement error hierarchy** using `thiserror`.
4. **Build prompt normalizer** converting `GenerateOptions` builder state into `Vec<Message>`.
5. **Implement provider registry** with thread-safe storage and global setter/getter.
6. **Implement high-level generate API** without retry/telemetry first; add unit tests using mock model implementing `LanguageModel`.
7. **Add retry utility** and integrate into `generate`.
8. **Implement stream API** with mock stream; ensure `StreamHandle::collect_text` produces identical result to `generate` for deterministic models.
9. **OpenAI provider**:
   - Config builder + env loader.
   - HTTP client creation with `reqwest`.
   - Request/response structs for `/responses` endpoint.
   - Implement `LanguageModel::generate` parsing JSON results, mapping errors.
   - Implement `stream` parsing SSE; convert to model chunks.
   - Unit tests using `wiremock` to simulate API responses.
10. **Integration tests** under `examples/tests` hitting live API behind feature flag or recorded fixtures.
11. **Telemetry hooks** using `tracing` spans in high-level APIs and provider calls (log request id).
12. **Documentation & Examples**: README with quickstart, environment setup, sample code for `generate` and `stream`.

## 7. API Examples (Sketch)
```rust
let registry = ProviderRegistry::global();
registry.register("openai", OpenAiProviderFactory::from_env()?);

let response = generate(
    GenerateOptions::new("openai:gpt-4o-mini")
        .system("You are a helpful assistant")
        .user("Summarize Rust async best practices")
        .temperature(0.3)
).await?;
println!("{}", response.text);

let mut stream = stream(
    StreamOptions::new("openai:gpt-4o-mini")
        .system("Speak in haiku")
        .user("Describe ownership")
).await?;

while let Some(chunk) = stream.next().await {
    match chunk? {
        StreamChunk::Text { delta } => print!("{}", delta),
        StreamChunk::Done { usage } => println!("\nTokens: {:?}", usage),
        _ => {}
    }
}
```

## 8. Testing Strategy
- **Unit tests** in core using `MockModel` that returns deterministic chunks.
- **Provider tests** with `wiremock` verifying request bodies and streaming handling.
- **End-to-end example** behind `cargo test -- --ignored` requiring real API key.
- **Fuzz tests** for stream parser to ensure robustness against malformed SSE frames (optional, nice-to-have).

## 9. Timeline (Condensed)
1. Core crate scaffolding & generate API – 1 week.
2. Streaming pipeline & mock tests – 1 week.
3. OpenAI provider implementation – 1.5 weeks.
4. Retry, telemetry, polish – 0.5 week.
5. Docs, examples, release prep – 0.5 week.

_Total ~4.5 weeks (single developer)._ 
