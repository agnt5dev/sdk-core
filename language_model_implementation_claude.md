# Language Model SDK Implementation Plan in Rust

## Overview
Build a Rust SDK that provides a unified interface for interacting with multiple LLM providers (OpenAI, Anthropic, Google), following the architecture pattern from the Vercel AI SDK.

## Core Architecture

### 1. Provider Abstraction Layer

```rust
// src/provider/mod.rs

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[async_trait]
pub trait LanguageModel: Send + Sync {
    /// Non-streaming generation
    async fn do_generate(&self, options: CallOptions) -> Result<GenerateResult, Error>;

    /// Streaming generation
    async fn do_stream(&self, options: CallOptions) -> Result<StreamResult, Error>;

    /// Provider name for logging
    fn provider(&self) -> &str;

    /// Model identifier
    fn model_id(&self) -> &str;

    /// Supported URL patterns for native handling
    fn supported_urls(&self) -> HashMap<String, Vec<regex::Regex>>;
}

pub struct CallOptions {
    pub prompt: Prompt,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub tools: Option<Vec<Tool>>,
    pub tool_choice: Option<ToolChoice>,
    pub response_format: Option<ResponseFormat>,
    pub stop_sequences: Option<Vec<String>>,
    pub seed: Option<u64>,
}

pub struct GenerateResult {
    pub content: Vec<Content>,
    pub finish_reason: FinishReason,
    pub usage: Usage,
    pub provider_metadata: Option<serde_json::Value>,
}

pub enum Content {
    Text { text: String },
    ToolCall { id: String, name: String, arguments: serde_json::Value },
    Image { data: Vec<u8>, mime_type: String },
}
```

### 2. High-Level API Functions

```rust
// src/api/mod.rs

/// Generate text (non-streaming)
pub async fn generate_text(
    model: &dyn LanguageModel,
    config: GenerateTextConfig,
) -> Result<TextResult, Error> {
    // Handle retries, telemetry, common logic
    // Call model.do_generate()
    // Process and return result
}

/// Stream text generation
pub async fn stream_text(
    model: &dyn LanguageModel,
    config: StreamTextConfig,
) -> Result<TextStream, Error> {
    // Handle streaming setup
    // Call model.do_stream()
    // Return wrapped stream
}

/// Generate structured object
pub async fn generate_object<T: DeserializeOwned + JsonSchema>(
    model: &dyn LanguageModel,
    config: GenerateObjectConfig,
) -> Result<ObjectResult<T>, Error> {
    // Inject schema into prompt
    // Call model.do_generate()
    // Parse and validate response
}

/// Stream structured object
pub async fn stream_object<T: DeserializeOwned + JsonSchema>(
    model: &dyn LanguageModel,
    config: StreamObjectConfig,
) -> Result<ObjectStream<T>, Error> {
    // Similar to generate_object but streaming
}
```

## Implementation Phases

### Phase 1: Core Foundation (Week 1)

**Goals:**
- Set up project structure
- Define core traits and types
- Implement error handling

**Tasks:**
1. Initialize Rust project with dependencies
2. Define `LanguageModel` trait
3. Create type definitions for:
   - `Prompt`, `Message`, `Content`
   - `Tool`, `ToolCall`, `ToolResult`
   - `Usage`, `FinishReason`
   - Error types using `thiserror`
4. Set up logging with `tracing`

**File Structure:**
```
src/
├── lib.rs
├── error.rs           # Error types
├── provider/
│   ├── mod.rs        # LanguageModel trait
│   └── types.rs      # Core type definitions
└── utils/
    ├── mod.rs
    └── retry.rs      # Retry logic
```

### Phase 2: First Provider - OpenAI (Week 2)

**Goals:**
- Implement OpenAI provider
- Test basic generation

**Tasks:**
1. Create `OpenAIProvider` struct
2. Implement `LanguageModel` trait for OpenAI
3. Handle API authentication
4. Map OpenAI-specific types to generic types
5. Implement both `do_generate` and `do_stream`
6. Write integration tests

**Code Structure:**
```rust
// src/providers/openai.rs

pub struct OpenAIProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl OpenAIProvider {
    pub fn new(api_key: String) -> Self { ... }

    async fn call_api(&self, body: OpenAIRequest) -> Result<OpenAIResponse> { ... }

    fn convert_prompt(&self, prompt: Prompt) -> OpenAIMessages { ... }
}

#[async_trait]
impl LanguageModel for OpenAIProvider {
    async fn do_generate(&self, options: CallOptions) -> Result<GenerateResult> {
        // Convert options to OpenAI format
        // Make API call
        // Convert response to generic format
    }

    async fn do_stream(&self, options: CallOptions) -> Result<StreamResult> {
        // Similar but with SSE handling
    }
}
```

### Phase 3: High-Level APIs (Week 3)

**Goals:**
- Implement the four main API functions
- Add retry logic and telemetry

**Tasks:**
1. Implement `generate_text` with:
   - Retry logic using exponential backoff
   - Request/response logging
   - Error handling
2. Implement `stream_text` with:
   - Stream transformation
   - Backpressure handling
3. Implement `generate_object` with:
   - Schema injection strategies
   - JSON parsing and validation
4. Implement `stream_object` with:
   - Incremental JSON parsing
   - Schema validation

**Key Components:**
```rust
// src/api/generate_text.rs

pub struct GenerateTextConfig {
    pub prompt: Prompt,
    pub max_retries: u32,
    pub on_step_finish: Option<Box<dyn Fn(StepResult)>>,
}

pub async fn generate_text(
    model: &dyn LanguageModel,
    config: GenerateTextConfig,
) -> Result<TextResult> {
    let mut retries = 0;
    loop {
        match model.do_generate(options).await {
            Ok(result) => return Ok(process_result(result)),
            Err(e) if retries < config.max_retries => {
                retries += 1;
                sleep(backoff_duration(retries)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Phase 4: Object Generation & Schema Support (Week 4)

**Goals:**
- Implement schema-based generation
- Add JSON Schema support

**Tasks:**
1. Integrate `schemars` for JSON Schema generation
2. Implement schema injection strategies:
   - Tool-based (for models with function calling)
   - Prompt-based (for others)
3. Add response parsing with error recovery
4. Implement partial object streaming

**Example:**
```rust
// src/api/generate_object.rs

pub async fn generate_object<T>(
    model: &dyn LanguageModel,
    config: GenerateObjectConfig,
) -> Result<ObjectResult<T>>
where
    T: DeserializeOwned + JsonSchema,
{
    let schema = schema_for!(T);

    // Inject schema into prompt
    let modified_options = inject_schema(config.options, schema);

    // Generate
    let result = model.do_generate(modified_options).await?;

    // Extract and parse JSON
    let json_str = extract_json(&result.content)?;
    let object: T = serde_json::from_str(&json_str)?;

    Ok(ObjectResult { object, usage: result.usage })
}
```

### Phase 5: Additional Providers (Week 5)

**Goals:**
- Add Anthropic and Google providers
- Ensure API consistency

**Tasks:**
1. Implement `AnthropicProvider`
   - Handle Claude-specific message format
   - Map streaming responses
2. Implement `GoogleProvider`
   - Handle Gemini API specifics
3. Create provider factory function
4. Write cross-provider tests

### Phase 6: Advanced Features (Week 6)

**Goals:**
- Add tool calling support
- Implement middleware system

**Tasks:**
1. Tool execution framework:
   ```rust
   pub trait Tool {
       async fn execute(&self, input: Value) -> Result<Value>;
       fn schema(&self) -> ToolSchema;
   }
   ```
2. Middleware for:
   - Logging
   - Rate limiting
   - Caching
3. Streaming utilities:
   - Text smoothing
   - Progress callbacks

## Dependencies

```toml
[dependencies]
# Core
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
futures = "0.3"

# HTTP & Streaming
reqwest = { version = "0.11", features = ["json", "stream"] }
tokio-stream = "0.1"
eventsource-stream = "0.2"

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
schemars = "0.8"

# Error handling
thiserror = "1.0"
anyhow = "1.0"

# Utilities
tracing = "0.1"
regex = "1.5"
once_cell = "1.19"

[dev-dependencies]
tokio-test = "0.4"
mockito = "1.0"
```

## Testing Strategy

### Unit Tests
- Test each provider's type conversions
- Test retry logic
- Test schema injection

### Integration Tests
```rust
#[tokio::test]
async fn test_openai_generation() {
    let provider = OpenAIProvider::new(env::var("OPENAI_API_KEY").unwrap());
    let result = generate_text(&provider, config).await.unwrap();
    assert!(!result.text.is_empty());
}
```

### Cross-Provider Tests
```rust
async fn test_all_providers(provider: &dyn LanguageModel) {
    // Test common functionality across all providers
}
```

## Performance Considerations

1. **Connection Pooling**: Reuse HTTP clients
2. **Streaming**: Use bounded channels to prevent memory issues
3. **Async Runtime**: Configure Tokio for optimal performance
4. **Zero-Copy**: Use `Bytes` for large responses

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Provider error: {0}")]
    Provider(String),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Parse error: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("Schema validation failed")]
    SchemaValidation,

    #[error("Rate limit exceeded")]
    RateLimit { retry_after: Duration },
}
```

## Documentation

Each public API should have:
- Purpose and use case
- Example code
- Parameter descriptions
- Error conditions

Example:
```rust
/// Generate text from a prompt using the specified language model.
///
/// # Example
/// ```rust
/// let provider = OpenAIProvider::new(api_key);
/// let result = generate_text(&provider, GenerateTextConfig {
///     prompt: "Hello, world!".into(),
///     max_tokens: Some(100),
///     ..Default::default()
/// }).await?;
/// println!("{}", result.text);
/// ```
pub async fn generate_text(...) -> Result<TextResult>
```

## Future Enhancements

1. **Caching Layer**: Cache responses for identical requests
2. **Observability**: OpenTelemetry integration
3. **Plugin System**: Allow custom providers
4. **CLI Tool**: Command-line interface for testing
5. **WASM Support**: Compile to WebAssembly for browser use

## Success Metrics

- All providers pass common test suite
- Streaming performance < 50ms latency
- Memory usage stable under load
- API ergonomics validated through examples
- Documentation coverage > 90%

## Timeline

- Week 1: Core foundation
- Week 2: OpenAI provider
- Week 3: High-level APIs
- Week 4: Object generation
- Week 5: Additional providers
- Week 6: Advanced features
- Week 7: Testing & documentation
- Week 8: Performance optimization & release

This plan provides a solid foundation for building a production-ready language model SDK in Rust, with clear separation of concerns and extensibility for future providers and features.