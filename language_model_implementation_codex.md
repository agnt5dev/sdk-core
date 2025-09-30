# Rust AI SDK Language Model Implementation Plan

## 1. Objectives
- Recreate the AI SDK core language model capabilities (`generate_text`, `stream_text`, `generate_object`, `stream_object`) in Rust.
- Maintain provider-agnostic high-level APIs while allowing provider-specific integrations (OpenAI, Bedrock, etc.) through a shared trait contract.
- Support prompt normalization, tool calling, structured outputs, streaming, retries, telemetry, and error wrapping equivalent to the existing TypeScript SDK.

## 2. Architecture Overview
- **Core Crate (`ai_sdk_core`)**: Houses shared traits, types, utilities, and high-level APIs. Publishes the user-facing interface.
- **Provider Crates (`ai_sdk_provider_openai`, `ai_sdk_provider_bedrock`, ...)**: Implement the `LanguageModel` trait and expose provider-specific configuration builders.
- **Gateway Provider**: Optional default provider replicating the Vercel AI Gateway; packaged as `ai_sdk_provider_gateway`.
- **Common Utilities**:
  - Retry + backoff (`ai_sdk_core::util::retry`)
  - Telemetry instrumentation (`ai_sdk_core::telemetry` wrapping `opentelemetry`)
  - Prompt tooling (`ai_sdk_core::prompt`)
  - Tool execution subsystem (`ai_sdk_core::tools`)
  - JSON schema helpers (`ai_sdk_core::json` using `schemars`/`serde_json`)

## 3. Trait Contracts
### 3.1 `LanguageModel`
```rust
#[async_trait]
pub trait LanguageModel: Send + Sync {
    fn provider(&self) -> &str;
    fn model_id(&self) -> &str;
    fn supported_urls(&self) -> SupportedUrlMap;

    async fn generate(
        &self,
        request: GenerateRequest,
    ) -> Result<GenerateResponse, LanguageModelError>;

    async fn stream(
        &self,
        request: StreamRequest,
    ) -> Result<StreamResponse, LanguageModelError>;
}
```
- `SupportedUrlMap` mirrors the TypeScript map from media type patterns to regexes.
- `GenerateRequest` & `StreamRequest` contain prompt, tool, sampling, telemetry, and provider options.
- `StreamResponse` carries a `Pin<Box<dyn Stream<Item = StreamChunk>>>` plus response metadata.

### 3.2 `Provider`
```rust
pub trait Provider: Send + Sync {
    fn language_model(&self, id: &str) -> Result<Arc<dyn LanguageModel>, ProviderError>;
    fn text_embedding_model(&self, id: &str) -> Result<Arc<dyn EmbeddingModel>, ProviderError>;
    // Optional: image, speech, transcription
}
```
- Registry holds named providers; `set_global_provider` sets the fallback used when callers pass string model IDs.

## 4. High-Level API Specifications
### 4.1 `generate_text`
- **Signature**
  ```rust
  pub async fn generate_text<Tools, Output>(options: GenerateTextOptions<Tools, Output>)
      -> Result<GenerateTextResult<Output, Tools>, GenerateTextError>;
  ```
- **Responsibilities**
  - Resolve model (string ID or `Arc<dyn LanguageModel>`).
  - Normalize prompt/messages (`PromptInput` → `PromptMessages`).
  - Prepare call settings (apply defaults, merge telemetry headers).
  - Configure tools & tool choice; orchestrate multi-step tool execution with retries.
  - Execute `LanguageModel::generate` for each step; aggregate text, reasoning, tool calls, usage.
  - Surface provider warnings, finish reason, raw response metadata.

### 4.2 `stream_text`
- **Signature**
  ```rust
  pub async fn stream_text<Tools>(
      options: StreamTextOptions<Tools>
  ) -> Result<StreamTextHandle<Tools>, StreamTextError>;
  ```
- **Responsibilities**
  - Same normalization/resolution as `generate_text`.
  - Call `LanguageModel::stream` and wrap the provider stream with adapters to:
    - Emit unified chunk enums (`TextDelta`, `ReasoningDelta`, `ToolCall`, `ToolResult`, `RawChunk`).
    - Execute registered tools when tool-call chunks appear.
    - Support optional smoothing/buffering strategies.
  - Provide handles for piping to HTTP responses or collecting the final result.

### 4.3 `generate_object`
- **Signature**
  ```rust
  pub async fn generate_object<T>(
      options: GenerateObjectOptions<T>
  ) -> Result<GenerateObjectResult<T>, GenerateObjectError>;
  ```
- **Responsibilities**
  - Validate schema/enum inputs and object generation mode (`Auto`, `Json`, `Tool`).
  - Construct JSON response format instructions and inject into prompt or tools based on provider capability.
  - Delegate to `LanguageModel::generate`, then run repair/parsing pipeline (JSON repair, schema validation via `serde`/`schemars`).

### 4.4 `stream_object`
- **Signature**
  ```rust
  pub async fn stream_object<T>(
      options: StreamObjectOptions<T>
  ) -> Result<StreamObjectHandle<T>, StreamObjectError>;
  ```
- **Responsibilities**
  - Similar to `generate_object`, but wrap the stream and emit partial JSON tokens, metadata, and final parsed object.
  - Expose `Delayed` style futures for usage, warnings, request/response metadata, plus a chunk stream.

## 5. Data Types & Modules
- `prompt` module: `PromptInput`, `PromptMessage`, converters, standardizers, support for system/prompt/messages.
- `tools` module: trait `Tool`, tool registry, tool execution runtime, error handling, active tool tracking.
- `usage` module: tracks token usage across steps.
- `telemetry`: wrappers over `tracing` + optional `opentelemetry` exporters.
- `retry`: exponential backoff with jitter using `tokio-retry` or `backoff` crate.
- `errors`: unified error enums with context (provider, model, request id).
- `download`: optional HTTP download service for media referenced in prompts, respecting `supported_urls` map.

## 6. Implementation Phases
1. **Foundations**
   - Define core enums/structs (`FinishReason`, `CallWarning`, `ToolChoice`, `ToolCall`, `Usage`, etc.).
   - Implement prompt normalization and model resolution (`ModelHandle`, `ProviderRegistry`).
   - Create `generate_text` skeleton calling `LanguageModel::generate` without tools.

2. **Streaming Support**
   - Implement `stream_text` pipeline with unified chunk enum and tool execution stubs.
   - Build `StreamTextHandle` for collecting result/metadata and piping to responses.

3. **Tools & Multi-Step Orchestration**
   - Add tool registry, execution runtime, and multi-step loop (retry on tool errors, stop conditions).
   - Support `StopCondition` strategies (max steps, custom predicate).

4. **Structured Outputs**
   - Introduce JSON schema strategies, repair functions, and `generate_object`/`stream_object` wrappers.
   - Integrate with tool system for tool-based schema enforcement.

5. **Provider Integrations**
   - Implement OpenAI provider crate (Responses API) leveraging `reqwest` + `tokio-stream`.
   - Add additional providers (Bedrock, Groq, etc.) following same trait contract.
   - Provide Gateway provider as default fallback.

6. **Telemetry & Observability**
   - Add tracing spans around each step (`generate`, `stream`, tool execution) with structured attributes.
   - Integrate configurable exporters (stdout, OTLP).

7. **Retry, Error Handling, Downloads**
   - Implement retry policies, download service for unsupported URLs, and wrap provider errors into SDK errors.

8. **Testing & Examples**
   - Unit tests for prompt conversion, tool execution, schema parsing.
   - Mock provider implementations for deterministic tests (synchronous and streaming).
   - Integration tests per provider with mocked HTTP servers (using `wiremock` or `httptest`).
   - Example binaries demonstrating each API.

## 7. Provider Implementation Blueprint (OpenAI Example)
- Config struct holds API base URL, key, headers, file id prefixes.
- `OpenAiLanguageModel` implements `LanguageModel::generate` by mapping `GenerateRequest` to `/responses` payload, handling warnings (unsupported settings), and parsing response JSON.
- `stream` method uses EventSource chunk decoding, translates chunks into `StreamChunk` enum, tracks tool-call state, finish reasons, usage.
- Provider factory exposes builders (`Provider::language_model`, `.chat`, `.responses`) returning `Arc<dyn LanguageModel>`.

## 8. Object Schema Strategy
- `OutputMode::Auto` selects best strategy based on provider capabilities declared via trait method `fn supported_output_modes(&self) -> OutputModeSet`.
- For JSON mode, inject `response_format` with strict JSON schema into request; for Tool mode, register a `Tool` with the schema.
- Schema conversion uses `schemars::schema_for!` to derive JSON Schema from Rust types; enums map to string unions.
- Repair step uses `jsonxf` or custom heuristics to fix minor JSON issues before validation.

## 9. Telemetry & Usage Tracking
- Maintain cumulative `Usage` across multi-step calls; each provider response populates tokens.
- Spans include standardized attributes (`gen_ai.system`, `gen_ai.request.*`, `ai.prompt.messages`, etc.).
- Streaming spans emit events for first chunk, tool call start/end, finish.

## 10. Configuration & Extensibility
- Global configuration struct (`AiSdkConfig`) with fields: default provider, download client, retry defaults, telemetry config.
- Allow per-call overrides via options structs.
- Feature flags for optional functionality (e.g., `telemetry`, `download`), enabling minimal builds.

## 11. Deliverables
- `ai_sdk_core` crate with high-level APIs, shared types, documentation.
- `ai_sdk_provider_openai` crate implementing Responses API.
- `ai_sdk_provider_gateway` crate for default provider.
- Comprehensive tests and docs (README, API reference, architecture guide).
- Example binaries demonstrating text completion, streaming, structured output, and tool calling.

## 12. Timeline Estimate
1. Foundations & API scaffolding – 2 weeks
2. Streaming & tool orchestration – 2 weeks
3. Structured output & schema handling – 1.5 weeks
4. OpenAI provider implementation – 1.5 weeks
5. Additional provider templates & gateway – 1 week
6. Telemetry, retries, downloads – 1 week
7. Testing hardening & docs – 1 week

_Total: ~10 weeks (parallelizable by splitting providers & tooling)._ 

