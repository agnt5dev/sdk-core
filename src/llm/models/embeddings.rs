// Embeddings models for vector representations
use super::EmbeddingUsage;
use serde::{Deserialize, Serialize};

/// Input data for embeddings - can be string or array of strings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EmbeddingsInput {
    String(String),
    Array(Vec<String>),
}

impl EmbeddingsInput {
    pub fn len(&self) -> usize {
        match self {
            EmbeddingsInput::String(_) => 1,
            EmbeddingsInput::Array(arr) => arr.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            EmbeddingsInput::String(s) => s.is_empty(),
            EmbeddingsInput::Array(arr) => arr.is_empty(),
        }
    }
}

/// Embeddings request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsRequest {
    pub model: String,
    pub input: EmbeddingsInput,

    // Optional parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_format: Option<String>, // "float", "base64"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u32>, // For models that support dimension reduction
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// A single embedding vector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    pub index: u32,
    pub embedding: Vec<f32>,
}

/// Complete embeddings response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    pub data: Vec<EmbeddingData>,
    pub model: String,
    pub usage: EmbeddingUsage,
}

impl EmbeddingsResponse {
    /// Get the first embedding vector if available
    pub fn first_embedding(&self) -> Option<&Vec<f32>> {
        self.data.first().map(|data| &data.embedding)
    }

    /// Get all embedding vectors
    pub fn embeddings(&self) -> Vec<&Vec<f32>> {
        self.data.iter().map(|data| &data.embedding).collect()
    }

    /// Get the dimension of the embeddings (assumes all are the same)
    pub fn dimension(&self) -> Option<usize> {
        self.data.first().map(|data| data.embedding.len())
    }
}
