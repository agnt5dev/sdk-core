use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::anyhow;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde_json::Value;

use crate::error::{Result as SdkResult, SdkError};

#[async_trait]
pub trait LanguageModel: Send + Sync {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse>;
    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle>;
}

pub async fn generate<M>(model: &M, request: GenerateRequest) -> SdkResult<GenerateResponse>
where
    M: LanguageModel,
{
    model.generate(request).await
}

pub async fn stream<M>(model: &M, request: StreamRequest) -> SdkResult<StreamHandle>
where
    M: LanguageModel,
{
    model.stream(request).await
}

#[derive(Clone, Debug)]
pub struct GenerateRequest {
    pub model: String,
    pub system_prompt: Option<String>,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDefinition>,
    pub tool_choice: Option<ToolChoice>,
    pub user_id: Option<String>,
    pub config: GenerationConfig,
    /// OpenTelemetry context for trace propagation across async boundaries
    /// This is used internally to ensure LM spans are children of the calling function span
    #[doc(hidden)]
    pub otel_context: Option<opentelemetry::Context>,
}

impl GenerateRequest {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            system_prompt: None,
            messages: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            user_id: None,
            config: GenerationConfig::default(),
            otel_context: None,
        }
    }

    pub fn system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    pub fn message(mut self, message: Message) -> Self {
        self.messages.push(message);
        self
    }

    pub fn system_message(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message::system(content));
        self
    }

    pub fn user_message(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message::user(content));
        self
    }

    pub fn assistant_message(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message::assistant(content));
        self
    }

    pub fn add_tool(mut self, tool: ToolDefinition) -> Self {
        self.tools.push(tool);
        self
    }

    pub fn tools(mut self, tools: Vec<ToolDefinition>) -> Self {
        self.tools = tools;
        self
    }

    pub fn tool_choice(mut self, choice: Option<ToolChoice>) -> Self {
        self.tool_choice = choice;
        self
    }

    pub fn user_id(mut self, value: impl Into<String>) -> Self {
        self.user_id = Some(value.into());
        self
    }

    pub fn configure<F>(mut self, configure: F) -> Self
    where
        F: FnOnce(&mut GenerationConfig),
    {
        configure(&mut self.config);
        self
    }

    pub fn with_config(mut self, config: GenerationConfig) -> Self {
        self.config = config;
        self
    }

    pub fn response_format(mut self, format: ResponseFormat) -> Self {
        self.config.response_format = format;
        self
    }
}

pub type StreamRequest = GenerateRequest;

#[derive(Clone, Debug, Default)]
pub struct GenerationConfig {
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_output_tokens: Option<u32>,
    pub response_format: ResponseFormat,
}

impl GenerationConfig {
    pub fn temperature(mut self, value: f32) -> Self {
        self.temperature = Some(value);
        self
    }

    pub fn top_p(mut self, value: f32) -> Self {
        self.top_p = Some(value);
        self
    }

    pub fn max_output_tokens(mut self, value: u32) -> Self {
        self.max_output_tokens = Some(value);
        self
    }

    pub fn response_format(mut self, format: ResponseFormat) -> Self {
        self.response_format = format;
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResponseFormat {
    Text,
    Json,
    JsonSchema(JsonSchemaFormat),
}

impl Default for ResponseFormat {
    fn default() -> Self {
        ResponseFormat::Text
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct JsonSchemaFormat {
    pub name: String,
    pub schema: Value,
    pub strict: bool,
}

impl JsonSchemaFormat {
    pub fn new(name: impl Into<String>, schema: Value) -> Self {
        Self {
            name: name.into(),
            schema,
            strict: true,
        }
    }

    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    pub role: MessageRole,
    pub content: String,
}

impl Message {
    pub fn new(role: MessageRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self::new(MessageRole::System, content)
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::new(MessageRole::User, content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new(MessageRole::Assistant, content)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageRole {
    System,
    User,
    Assistant,
}

impl MessageRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub parameters: Option<Value>,
    pub strict: Option<bool>,
}

impl ToolDefinition {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            parameters: None,
            strict: None,
        }
    }

    pub fn description(mut self, text: impl Into<String>) -> Self {
        self.description = Some(text.into());
        self
    }

    pub fn parameters(mut self, parameters: Value) -> Self {
        self.parameters = Some(parameters);
        self
    }

    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = Some(strict);
        self
    }
}

#[derive(Clone, Debug)]
pub enum ToolChoice {
    Auto,
    None,
    Tool { name: String },
}

#[derive(Clone, Debug)]
pub struct GenerateResponse {
    pub id: String,
    pub model: String,
    pub created: Option<u64>,
    pub text: String,
    pub usage: Option<TokenUsage>,
    pub finish_reason: Option<String>,
    pub object: Option<Value>,
    pub raw: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct TokenUsage {
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub total_tokens: Option<u32>,
}

pub enum StreamChunk {
    Delta { content: String },
    Completed(GenerateResponse),
}

pub struct StreamHandle {
    inner: Pin<Box<dyn Stream<Item = SdkResult<StreamChunk>> + Send>>,
}

impl StreamHandle {
    pub(crate) fn new(inner: Pin<Box<dyn Stream<Item = SdkResult<StreamChunk>> + Send>>) -> Self {
        Self { inner }
    }

    pub async fn collect_text(mut self) -> SdkResult<GenerateResponse> {
        let mut final_response: Option<GenerateResponse> = None;
        while let Some(item) = self.inner.next().await {
            match item? {
                StreamChunk::Delta { .. } => {}
                StreamChunk::Completed(response) => {
                    final_response = Some(response);
                    break;
                }
            }
        }

        final_response
            .ok_or_else(|| SdkError::Other(anyhow!("stream ended without a completion response")))
    }

    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = SdkResult<StreamChunk>> + Send>> {
        self.inner
    }
}

impl Stream for StreamHandle {
    type Item = SdkResult<StreamChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safety: we never move the inner stream after pinning.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_next(cx)
    }
}
