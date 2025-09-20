// Streaming response models for real-time LLM interactions
use serde::{Deserialize, Serialize};
use super::{Usage, ToolCall};

/// Delta message for streaming chat completions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessageDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

/// Choice in a streaming chat completion chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionChunkChoice {
    pub index: u32,
    pub delta: ChatMessageDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<serde_json::Value>,
}

/// A single chunk in a streaming chat completion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<u64>,
    pub model: String,
    pub choices: Vec<ChatCompletionChunkChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>, // Only present in the final chunk
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
}

/// Choice in a streaming completion chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionChunkChoice {
    pub index: u32,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<serde_json::Value>,
}

/// A single chunk in a streaming completion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionChunk {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<u64>,
    pub model: String,
    pub choices: Vec<CompletionChunkChoice>,
}

/// Generic streaming response wrapper
#[derive(Debug)]
pub enum StreamingResponse<T> {
    Chunk(T),
    Error(crate::error::SdkError),
    Done,
}

impl<T> StreamingResponse<T> {
    pub fn is_chunk(&self) -> bool {
        matches!(self, StreamingResponse::Chunk(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, StreamingResponse::Error(_))
    }

    pub fn is_done(&self) -> bool {
        matches!(self, StreamingResponse::Done)
    }

    pub fn into_chunk(self) -> Option<T> {
        match self {
            StreamingResponse::Chunk(chunk) => Some(chunk),
            _ => None,
        }
    }

    pub fn into_error(self) -> Option<crate::error::SdkError> {
        match self {
            StreamingResponse::Error(err) => Some(err),
            _ => None,
        }
    }
}

/// Helper to accumulate streaming chunks into final response
#[derive(Debug)]
pub struct ChatCompletionAccumulator {
    pub id: String,
    pub model: String,
    pub choices: Vec<AccumulatedChoice>,
    pub usage: Usage,
    pub system_fingerprint: Option<String>,
}

#[derive(Debug)]
pub struct AccumulatedChoice {
    pub index: u32,
    pub content: String,
    pub role: String,
    pub tool_calls: Option<Vec<ToolCall>>,
    pub finish_reason: Option<String>,
}

impl ChatCompletionAccumulator {
    pub fn new(first_chunk: &ChatCompletionChunk) -> Self {
        let choices = first_chunk.choices.iter().map(|choice| {
            AccumulatedChoice {
                index: choice.index,
                content: choice.delta.content.clone().unwrap_or_default(),
                role: choice.delta.role.clone().unwrap_or_else(|| "assistant".to_string()),
                tool_calls: choice.delta.tool_calls.clone(),
                finish_reason: choice.finish_reason.clone(),
            }
        }).collect();

        Self {
            id: first_chunk.id.clone(),
            model: first_chunk.model.clone(),
            choices,
            usage: first_chunk.usage.clone().unwrap_or_default(),
            system_fingerprint: first_chunk.system_fingerprint.clone(),
        }
    }

    pub fn add_chunk(&mut self, chunk: &ChatCompletionChunk) {
        for chunk_choice in &chunk.choices {
            if let Some(existing_choice) = self.choices.get_mut(chunk_choice.index as usize) {
                if let Some(content) = &chunk_choice.delta.content {
                    existing_choice.content.push_str(content);
                }
                if chunk_choice.finish_reason.is_some() {
                    existing_choice.finish_reason = chunk_choice.finish_reason.clone();
                }
                if let Some(tool_calls) = &chunk_choice.delta.tool_calls {
                    existing_choice.tool_calls = Some(tool_calls.clone());
                }
            }
        }

        // Update usage if present (usually only in final chunk)
        if let Some(usage) = &chunk.usage {
            self.usage = usage.clone();
        }
    }
}