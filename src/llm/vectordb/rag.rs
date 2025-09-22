// RAG (Retrieval-Augmented Generation) pipeline components
use std::sync::Arc;
use unicode_segmentation::UnicodeSegmentation;

use super::super::{ChatCompletionRequest, ChatMessage, ChatMessageContent, LlmClient};
use super::{SearchQuery, SearchResult, VectorDatabase, VectorEntry, VectorMetadata};
use crate::error::{Result, SdkError};

/// Document processing and chunking for RAG pipelines
#[derive(Debug, Clone)]
pub struct DocumentProcessor {
    /// Maximum chunk size in characters
    pub max_chunk_size: usize,

    /// Chunk overlap in characters
    pub chunk_overlap: usize,

    /// Separator patterns for splitting
    pub separators: Vec<String>,
}

impl DocumentProcessor {
    pub fn new() -> Self {
        Self {
            max_chunk_size: 1000,
            chunk_overlap: 200,
            separators: vec![
                "\n\n".to_string(), // Paragraphs
                "\n".to_string(),   // Lines
                ". ".to_string(),   // Sentences
                " ".to_string(),    // Words
            ],
        }
    }

    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.max_chunk_size = size;
        self
    }

    pub fn with_overlap(mut self, overlap: usize) -> Self {
        self.chunk_overlap = overlap;
        self
    }

    /// Split text into chunks with metadata
    pub fn chunk_text(&self, text: &str, source: Option<String>) -> Vec<DocumentChunk> {
        let chunks = self.split_text(text);

        chunks
            .into_iter()
            .enumerate()
            .map(|(index, chunk_text)| DocumentChunk {
                text: chunk_text,
                metadata: VectorMetadata::new()
                    .with_chunk_index(index as u32)
                    .with_source(source.clone().unwrap_or_else(|| "unknown".to_string())),
            })
            .collect()
    }

    fn split_text(&self, text: &str) -> Vec<String> {
        if text.len() <= self.max_chunk_size {
            return vec![text.to_string()];
        }

        let mut chunks = Vec::new();

        // Try each separator in order
        for separator in &self.separators {
            if self.try_split_by_separator(text, separator, &mut chunks) {
                break;
            }
        }

        // If no separator worked, fall back to character-based splitting
        if chunks.is_empty() {
            chunks = self.split_by_characters(text);
        }

        chunks
    }

    fn try_split_by_separator(
        &self,
        text: &str,
        separator: &str,
        chunks: &mut Vec<String>,
    ) -> bool {
        let parts: Vec<&str> = text.split(separator).collect();

        if parts.len() <= 1 {
            return false;
        }

        let mut current_chunk = String::new();

        for part in parts {
            let potential_chunk = if current_chunk.is_empty() {
                part.to_string()
            } else {
                format!("{}{}{}", current_chunk, separator, part)
            };

            if potential_chunk.len() <= self.max_chunk_size {
                current_chunk = potential_chunk;
            } else {
                if !current_chunk.is_empty() {
                    chunks.push(current_chunk);
                }
                current_chunk = part.to_string();

                // If single part is too large, we'll need to split it further
                if current_chunk.len() > self.max_chunk_size {
                    let sub_chunks = self.split_by_characters(&current_chunk);
                    chunks.extend(sub_chunks);
                    current_chunk = String::new();
                }
            }
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        true
    }

    fn split_by_characters(&self, text: &str) -> Vec<String> {
        let mut chunks = Vec::new();
        let graphemes: Vec<&str> = text.graphemes(true).collect();

        let mut start = 0;
        while start < graphemes.len() {
            let end = std::cmp::min(start + self.max_chunk_size, graphemes.len());
            let chunk = graphemes[start..end].join("");
            chunks.push(chunk);

            // Move forward with overlap
            start = if end - start > self.chunk_overlap {
                end - self.chunk_overlap
            } else {
                end
            };
        }

        chunks
    }
}

impl Default for DocumentProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// A document chunk with associated metadata
#[derive(Debug, Clone)]
pub struct DocumentChunk {
    pub text: String,
    pub metadata: VectorMetadata,
}

/// RAG pipeline for retrieval-augmented generation
pub struct RagPipeline {
    llm_client: Arc<LlmClient>,
    vector_db: Arc<dyn VectorDatabase>,
    document_processor: DocumentProcessor,
    embedding_model: String,
    llm_provider: String,
    collection_name: String,
}

impl RagPipeline {
    pub fn new(
        llm_client: Arc<LlmClient>,
        vector_db: Arc<dyn VectorDatabase>,
        embedding_model: String,
        llm_provider: String,
        collection_name: String,
    ) -> Self {
        Self {
            llm_client,
            vector_db,
            document_processor: DocumentProcessor::new(),
            embedding_model,
            llm_provider,
            collection_name,
        }
    }

    pub fn with_document_processor(mut self, processor: DocumentProcessor) -> Self {
        self.document_processor = processor;
        self
    }

    /// Ingest a document into the vector database
    pub async fn ingest_document(&self, text: &str, source: Option<String>) -> Result<()> {
        // 1. Chunk the document
        let chunks = self.document_processor.chunk_text(text, source);

        // 2. Generate embeddings for each chunk
        let mut vector_entries = Vec::new();

        for chunk in chunks {
            let embedding_request = super::super::models::EmbeddingsRequest {
                model: self.embedding_model.clone(),
                input: super::super::models::EmbeddingsInput::String(chunk.text.clone()),
                encoding_format: None,
                dimensions: None,
                user: None,
            };

            let embedding_response = self
                .llm_client
                .embeddings(&self.llm_provider, embedding_request)
                .await?;

            if let Some(embedding) = embedding_response.first_embedding() {
                let vector_entry = VectorEntry {
                    id: uuid::Uuid::new_v4().to_string(),
                    vector: embedding.clone(),
                    metadata: chunk.metadata.with_text(chunk.text),
                };
                vector_entries.push(vector_entry);
            }
        }

        // 3. Store vectors in the database
        self.vector_db
            .upsert_vectors(&self.collection_name, vector_entries)
            .await?;

        Ok(())
    }

    /// Perform retrieval-augmented generation
    pub async fn query(
        &self,
        question: &str,
        num_results: u32,
        model: &str,
    ) -> Result<RagResponse> {
        // 1. Generate embedding for the question
        let question_embedding_request = super::super::models::EmbeddingsRequest {
            model: self.embedding_model.clone(),
            input: super::super::models::EmbeddingsInput::String(question.to_string()),
            encoding_format: None,
            dimensions: None,
            user: None,
        };

        let question_embedding_response = self
            .llm_client
            .embeddings(&self.llm_provider, question_embedding_request)
            .await?;

        let question_vector = question_embedding_response
            .first_embedding()
            .ok_or_else(|| {
                SdkError::Other(anyhow::anyhow!("Failed to generate question embedding"))
            })?;

        // 2. Search for relevant documents
        let search_query = SearchQuery::new(question_vector.clone())
            .with_limit(num_results)
            .include_metadata();

        let search_results = self
            .vector_db
            .search_vectors(&self.collection_name, search_query)
            .await?;

        // 3. Build context from search results
        let context = self.build_context(&search_results);

        // 4. Generate response with LLM
        let prompt = self.build_prompt(question, &context);

        let chat_request = ChatCompletionRequest {
            model: model.to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(ChatMessageContent::String(prompt)),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                refusal: None,
            }],
            temperature: Some(0.7),
            max_tokens: Some(2000),
            stream: Some(false),
            top_p: None,
            n: None,
            stop: None,
            max_completion_tokens: None,
            presence_penalty: None,
            frequency_penalty: None,
            tools: None,
            tool_choice: None,
            response_format: None,
            reasoning: None,
            logprobs: None,
            top_logprobs: None,
            seed: None,
            user: None,
        };

        let chat_response = self
            .llm_client
            .chat_completion(&self.llm_provider, chat_request)
            .await?;

        // 5. Extract response text
        let response_text = match chat_response {
            super::super::ChatCompletionResponse::NonStream(completion) => completion
                .choices
                .first()
                .and_then(|choice| choice.message.content.as_ref())
                .and_then(|content| match content {
                    ChatMessageContent::String(text) => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "No response generated".to_string()),
            super::super::ChatCompletionResponse::Stream(_) => {
                return Err(SdkError::Other(anyhow::anyhow!(
                    "Streaming not supported in RAG pipeline yet"
                )));
            }
        };

        Ok(RagResponse {
            answer: response_text,
            sources: search_results,
            context,
        })
    }

    fn build_context(&self, search_results: &[SearchResult]) -> String {
        let context_parts: Vec<String> = search_results
            .iter()
            .filter_map(|result| result.metadata.as_ref()?.text.as_ref())
            .map(|text| text.clone())
            .collect();

        context_parts.join("\n\n---\n\n")
    }

    fn build_prompt(&self, question: &str, context: &str) -> String {
        format!(
            "Use the following context to answer the question. If you cannot answer based on the context, say so.

Context:
{}

Question: {}

Answer:",
            context, question
        )
    }
}

/// Response from RAG pipeline
#[derive(Debug, Clone)]
pub struct RagResponse {
    /// Generated answer
    pub answer: String,

    /// Source documents used for generation
    pub sources: Vec<SearchResult>,

    /// Raw context that was provided to the LLM
    pub context: String,
}

/// Configuration for RAG pipeline
#[derive(Debug, Clone)]
pub struct RagConfig {
    /// Embedding model to use
    pub embedding_model: String,

    /// LLM provider for generation
    pub llm_provider: String,

    /// LLM model for generation
    pub llm_model: String,

    /// Collection name for vectors
    pub collection_name: String,

    /// Number of documents to retrieve
    pub num_results: u32,

    /// Document processing configuration
    pub document_processor: DocumentProcessor,
}

impl RagConfig {
    pub fn new(
        embedding_model: String,
        llm_provider: String,
        llm_model: String,
        collection_name: String,
    ) -> Self {
        Self {
            embedding_model,
            llm_provider,
            llm_model,
            collection_name,
            num_results: 5,
            document_processor: DocumentProcessor::new(),
        }
    }

    pub fn with_num_results(mut self, num_results: u32) -> Self {
        self.num_results = num_results;
        self
    }

    pub fn with_document_processor(mut self, processor: DocumentProcessor) -> Self {
        self.document_processor = processor;
        self
    }
}
