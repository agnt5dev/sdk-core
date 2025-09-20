// LLM model types and structures
pub mod chat;
pub mod completion;
pub mod embeddings;
pub mod streaming;
pub mod usage;

// Re-export commonly used types
pub use chat::{
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage,
    ChatMessageContent, ChatChoice, ReasoningConfig, ChatCompletion, ContentPart
};
pub use completion::{CompletionRequest, CompletionResponse, CompletionChoice};
pub use embeddings::{EmbeddingsRequest, EmbeddingsResponse, EmbeddingData, EmbeddingsInput};
pub use streaming::{StreamingResponse, ChatCompletionChunk, CompletionChunk};
pub use usage::{Usage, EmbeddingUsage};

// Tool types (defined in this module - exports only the alias)
pub use ToolDefinition as Tool;

// Choice type from chat module
pub use chat::ChatChoice as Choice;

// Common tool types
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub r#type: String,
    pub function: FunctionDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDefinition {
    pub name: String,
    pub description: Option<String>,
    pub parameters: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub r#type: String,
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolChoice {
    None,
    Auto,
    Required,
    Function { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFormat {
    pub r#type: String, // "text" or "json_object"
}

impl Default for ResponseFormat {
    fn default() -> Self {
        Self {
            r#type: "text".to_string(),
        }
    }
}