// SemanticMemory - High-level abstraction for vector-backed semantic memory
// Combines embeddings and vector database for simple memory operations

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Result, SdkError};
use crate::lm::{Embedder, EmbedderRegistry, OpenAiEmbedder};
use crate::vectordb::{
    Collection, DistanceMetric, SearchQuery, VectorDatabase, VectorDbRegistry, VectorEntry,
    VectorMetadata,
};

// ============================================================================
// Memory Scope
// ============================================================================

/// Scope of semantic memory determines data isolation and collection naming.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    /// User-scoped memory - isolated per user
    User,
    /// Tenant-scoped memory - shared across users in a tenant
    Tenant,
    /// Agent-scoped memory - specific to an agent
    Agent,
    /// Session-scoped memory - ephemeral, per session
    Session,
    /// Global memory - shared across all scopes (use with caution)
    Global,
}

impl MemoryScope {
    /// Convert scope to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryScope::User => "user",
            MemoryScope::Tenant => "tenant",
            MemoryScope::Agent => "agent",
            MemoryScope::Session => "session",
            MemoryScope::Global => "global",
        }
    }

    /// Parse scope from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "user" => Some(MemoryScope::User),
            "tenant" => Some(MemoryScope::Tenant),
            "agent" => Some(MemoryScope::Agent),
            "session" => Some(MemoryScope::Session),
            "global" => Some(MemoryScope::Global),
            _ => None,
        }
    }

    /// Build collection name from scope and ID
    /// Format: {scope}_{scope_id}_memories
    pub fn collection_name(&self, scope_id: &str) -> String {
        // Sanitize scope_id for use in collection name
        let sanitized_id: String = scope_id
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect();

        format!("{}_{}_memories", self.as_str(), sanitized_id)
    }
}

impl std::fmt::Display for MemoryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ============================================================================
// Memory Result
// ============================================================================

/// Result from a semantic memory search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryResult {
    /// Unique identifier for this memory
    pub id: String,

    /// Original text content that was stored
    pub content: String,

    /// Similarity score (0.0 to 1.0, higher is more similar)
    pub score: f32,

    /// Additional metadata stored with the memory
    pub metadata: MemoryMetadata,
}

/// Metadata associated with a memory entry
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryMetadata {
    /// Source of the memory (e.g., "conversation", "document", "user_input")
    pub source: Option<String>,

    /// Timestamp when memory was created (ISO 8601)
    pub created_at: Option<String>,

    /// Additional custom metadata
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

impl MemoryMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_created_at(mut self, timestamp: impl Into<String>) -> Self {
        self.created_at = Some(timestamp.into());
        self
    }

    pub fn with_extra<T: Serialize>(mut self, key: impl Into<String>, value: T) -> Self {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.extra.insert(key.into(), json_value);
        }
        self
    }
}

// ============================================================================
// SemanticMemory
// ============================================================================

/// Configuration for SemanticMemory
#[derive(Debug, Clone)]
pub struct SemanticMemoryConfig {
    /// Memory scope (user, tenant, agent, session)
    pub scope: MemoryScope,

    /// Identifier within the scope (e.g., user_id, tenant_id)
    pub scope_id: String,

    /// Distance metric for similarity search
    pub distance_metric: DistanceMetric,

    /// Whether to auto-create collections if they don't exist
    pub auto_create_collection: bool,
}

impl SemanticMemoryConfig {
    pub fn new(scope: MemoryScope, scope_id: impl Into<String>) -> Self {
        Self {
            scope,
            scope_id: scope_id.into(),
            distance_metric: DistanceMetric::Cosine,
            auto_create_collection: true,
        }
    }

    pub fn with_distance_metric(mut self, metric: DistanceMetric) -> Self {
        self.distance_metric = metric;
        self
    }

    pub fn with_auto_create(mut self, auto_create: bool) -> Self {
        self.auto_create_collection = auto_create;
        self
    }
}

/// SemanticMemory provides a high-level interface for vector-backed memory.
///
/// It combines an embedder (for generating vectors from text) with a vector
/// database (for storing and searching vectors) to provide simple semantic
/// memory operations.
///
/// # Example
///
/// ```ignore
/// use agnt5_sdk_core::memory::{SemanticMemory, MemoryScope};
///
/// // Create memory with auto-detection from environment
/// let memory = SemanticMemory::from_env(MemoryScope::User, "user-123").await?;
///
/// // Store a memory
/// let id = memory.store("User prefers dark mode").await?;
///
/// // Search for similar memories
/// let results = memory.search("color preferences", 5).await?;
///
/// // Delete a memory
/// memory.forget(&id).await?;
/// ```
pub struct SemanticMemory {
    config: SemanticMemoryConfig,
    embedder: Arc<dyn Embedder>,
    vectordb: Arc<dyn VectorDatabase>,
    collection_name: String,
    collection_initialized: std::sync::atomic::AtomicBool,
}

impl SemanticMemory {
    /// Create a new SemanticMemory with explicit embedder and vector database
    pub fn new(
        config: SemanticMemoryConfig,
        embedder: Arc<dyn Embedder>,
        vectordb: Arc<dyn VectorDatabase>,
    ) -> Self {
        let collection_name = config.scope.collection_name(&config.scope_id);

        Self {
            config,
            embedder,
            vectordb,
            collection_name,
            collection_initialized: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create SemanticMemory from environment variables
    ///
    /// This auto-detects available embedder and vector database providers:
    /// - Embedder: OPENAI_API_KEY
    /// - VectorDB: QDRANT_URL, PINECONE_API_KEY+PINECONE_HOST, POSTGRES_URL
    pub async fn from_env(scope: MemoryScope, scope_id: impl Into<String>) -> Result<Self> {
        let config = SemanticMemoryConfig::new(scope, scope_id);
        Self::from_env_with_config(config).await
    }

    /// Create SemanticMemory from environment with custom config
    pub async fn from_env_with_config(config: SemanticMemoryConfig) -> Result<Self> {
        // Load embedder from environment
        let embedder: Arc<dyn Embedder> = {
            let mut registry = EmbedderRegistry::new();
            registry.load_from_environment().map_err(|_| {
                SdkError::Configuration {
                    message: "No embedder configured. Set OPENAI_API_KEY for embeddings.".to_string(),
                    field: Some("OPENAI_API_KEY".to_string()),
                }
            })?;
            registry.get_default_provider().ok_or_else(|| {
                SdkError::Configuration {
                    message: "No default embedder provider available".to_string(),
                    field: None,
                }
            })?
        };

        // Load vector database from environment
        let vectordb: Arc<dyn VectorDatabase> = {
            let mut registry = VectorDbRegistry::new();
            registry.load_from_environment().await.map_err(|_| {
                SdkError::Configuration {
                    message: "No vector database configured. Set QDRANT_URL, PINECONE_API_KEY+PINECONE_HOST, or POSTGRES_URL.".to_string(),
                    field: None,
                }
            })?;
            registry.get_default_provider().ok_or_else(|| {
                SdkError::Configuration {
                    message: "No default vector database provider available".to_string(),
                    field: None,
                }
            })?
        };

        Ok(Self::new(config, embedder, vectordb))
    }

    /// Create SemanticMemory with OpenAI embeddings and a specific vector database
    pub fn with_openai_embeddings(
        config: SemanticMemoryConfig,
        vectordb: Arc<dyn VectorDatabase>,
    ) -> Result<Self> {
        let embedder = OpenAiEmbedder::from_env()?;
        Ok(Self::new(config, Arc::new(embedder), vectordb))
    }

    /// Ensure the collection exists (lazy initialization)
    async fn ensure_collection(&self) -> Result<()> {
        if self.collection_initialized.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        if self.config.auto_create_collection {
            let collection = Collection {
                name: self.collection_name.clone(),
                dimension: self.embedder.dimension(),
                distance_metric: self.config.distance_metric,
                description: Some(format!(
                    "Semantic memory for {} {}",
                    self.config.scope, self.config.scope_id
                )),
                config: std::collections::HashMap::new(),
            };

            // Try to create collection, ignore if it already exists
            match self.vectordb.create_collection(&collection).await {
                Ok(_) => {
                    tracing::info!(
                        collection = %self.collection_name,
                        scope = %self.config.scope,
                        scope_id = %self.config.scope_id,
                        "Created semantic memory collection"
                    );
                }
                Err(e) => {
                    // Many providers don't fail on duplicate creation, but some might
                    tracing::debug!(
                        collection = %self.collection_name,
                        error = %e,
                        "Collection creation note (may already exist)"
                    );
                }
            }
        }

        self.collection_initialized
            .store(true, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Store content in semantic memory
    ///
    /// Generates an embedding for the content and stores it in the vector database.
    /// Returns the unique ID of the stored memory.
    pub async fn store(&self, content: &str) -> Result<String> {
        self.store_with_metadata(content, MemoryMetadata::new()).await
    }

    /// Store multiple contents in batch (more efficient for RAG indexing)
    ///
    /// Uses batch embedding and batch upsert for better performance.
    /// Returns the unique IDs of all stored memories.
    pub async fn store_batch(&self, contents: &[&str]) -> Result<Vec<String>> {
        let metadata: Vec<MemoryMetadata> = contents.iter().map(|_| MemoryMetadata::new()).collect();
        self.store_batch_with_metadata(contents, &metadata).await
    }

    /// Store multiple contents with metadata in batch
    ///
    /// Each content must have a corresponding metadata entry.
    /// Returns the unique IDs of all stored memories.
    pub async fn store_batch_with_metadata(
        &self,
        contents: &[&str],
        metadata: &[MemoryMetadata],
    ) -> Result<Vec<String>> {
        if contents.len() != metadata.len() {
            return Err(SdkError::Other(anyhow::anyhow!(
                "Contents and metadata arrays must have the same length"
            )));
        }

        if contents.is_empty() {
            return Ok(vec![]);
        }

        let span = tracing::info_span!(
            "semantic_memory.store_batch",
            otel.name = "semantic_memory.store_batch",
            memory.scope = %self.config.scope,
            memory.scope_id = %self.config.scope_id,
            memory.collection = %self.collection_name,
            memory.batch_size = contents.len(),
        );
        let _enter = span.enter();

        self.ensure_collection().await?;

        // Batch embed all contents
        let vectors = self.embedder.embed_batch(contents).await.map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to generate batch embeddings: {}", e))
        })?;

        // Build vector entries
        let mut entries = Vec::with_capacity(contents.len());
        let mut ids = Vec::with_capacity(contents.len());
        let now = chrono::Utc::now().to_rfc3339();

        for (i, (content, vector)) in contents.iter().zip(vectors.into_iter()).enumerate() {
            let id = Uuid::new_v4().to_string();
            ids.push(id.clone());

            let meta = &metadata[i];
            let mut vector_metadata = VectorMetadata::new().with_text(content.to_string());

            if let Some(source) = &meta.source {
                vector_metadata = vector_metadata.with_source(source.clone());
            }

            for (key, value) in &meta.extra {
                vector_metadata.extra.insert(key.clone(), value.clone());
            }

            // Add timestamp
            let created_at = meta.created_at.clone().unwrap_or_else(|| now.clone());
            vector_metadata.extra.insert(
                "created_at".to_string(),
                serde_json::Value::String(created_at),
            );

            entries.push(VectorEntry {
                id,
                vector,
                metadata: vector_metadata,
            });
        }

        // Batch upsert to vector database
        self.vectordb
            .upsert_vectors(&self.collection_name, entries)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to store memories: {}", e)))?;

        tracing::debug!(
            collection = %self.collection_name,
            batch_size = contents.len(),
            "Stored semantic memory batch"
        );

        Ok(ids)
    }

    /// Store content with custom metadata
    pub async fn store_with_metadata(&self, content: &str, metadata: MemoryMetadata) -> Result<String> {
        let span = tracing::info_span!(
            "semantic_memory.store",
            otel.name = "semantic_memory.store",
            memory.scope = %self.config.scope,
            memory.scope_id = %self.config.scope_id,
            memory.collection = %self.collection_name,
            memory.content_length = content.len(),
            memory.id = tracing::field::Empty,
        );
        let _enter = span.enter();

        self.ensure_collection().await?;

        // Generate embedding
        let vector = self.embedder.embed(content).await.map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to generate embedding: {}", e))
        })?;

        // Generate unique ID
        let id = Uuid::new_v4().to_string();
        span.record("memory.id", &id);

        // Build vector metadata
        let mut vector_metadata = VectorMetadata::new().with_text(content.to_string());

        if let Some(source) = &metadata.source {
            vector_metadata = vector_metadata.with_source(source.clone());
        }

        for (key, value) in &metadata.extra {
            vector_metadata.extra.insert(key.clone(), value.clone());
        }

        // Add timestamp if not provided
        if metadata.created_at.is_none() {
            vector_metadata.extra.insert(
                "created_at".to_string(),
                serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
            );
        } else {
            vector_metadata.extra.insert(
                "created_at".to_string(),
                serde_json::Value::String(metadata.created_at.unwrap()),
            );
        }

        // Create vector entry
        let entry = VectorEntry {
            id: id.clone(),
            vector,
            metadata: vector_metadata,
        };

        // Upsert to vector database
        self.vectordb
            .upsert_vectors(&self.collection_name, vec![entry])
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to store memory: {}", e)))?;

        tracing::debug!(
            memory_id = %id,
            collection = %self.collection_name,
            content_length = content.len(),
            "Stored semantic memory"
        );

        Ok(id)
    }

    /// Search for similar memories
    ///
    /// Returns memories ranked by similarity to the query text.
    pub async fn search(&self, query: &str, limit: u32) -> Result<Vec<MemoryResult>> {
        self.search_with_options(query, limit, None).await
    }

    /// Search with additional options
    pub async fn search_with_options(
        &self,
        query: &str,
        limit: u32,
        min_score: Option<f32>,
    ) -> Result<Vec<MemoryResult>> {
        let span = tracing::info_span!(
            "semantic_memory.search",
            otel.name = "semantic_memory.search",
            memory.scope = %self.config.scope,
            memory.scope_id = %self.config.scope_id,
            memory.collection = %self.collection_name,
            memory.query_length = query.len(),
            memory.limit = limit,
            memory.min_score = min_score,
            memory.results_count = tracing::field::Empty,
        );
        let _enter = span.enter();

        self.ensure_collection().await?;

        // Generate embedding for query
        let query_vector = self.embedder.embed(query).await.map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to generate query embedding: {}", e))
        })?;

        // Build search query
        let search_query = SearchQuery {
            vector: query_vector,
            limit,
            min_score,
            filter: None,
            distance_metric: Some(self.config.distance_metric),
            include_vectors: false,
            include_metadata: true,
        };

        // Execute search
        let results = self
            .vectordb
            .search_vectors(&self.collection_name, search_query)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to search memories: {}", e)))?;

        // Convert to MemoryResult
        let memory_results: Vec<MemoryResult> = results
            .into_iter()
            .filter_map(|r| {
                let metadata = r.metadata?;
                let content = metadata.text?;

                Some(MemoryResult {
                    id: r.id,
                    content,
                    score: r.score,
                    metadata: MemoryMetadata {
                        source: metadata.source,
                        created_at: metadata
                            .extra
                            .get("created_at")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        extra: metadata.extra,
                    },
                })
            })
            .collect();

        span.record("memory.results_count", memory_results.len());

        tracing::debug!(
            collection = %self.collection_name,
            query_length = query.len(),
            results_count = memory_results.len(),
            "Semantic memory search completed"
        );

        Ok(memory_results)
    }

    /// Delete a memory by ID
    pub async fn forget(&self, memory_id: &str) -> Result<bool> {
        let span = tracing::info_span!(
            "semantic_memory.forget",
            otel.name = "semantic_memory.forget",
            memory.scope = %self.config.scope,
            memory.scope_id = %self.config.scope_id,
            memory.collection = %self.collection_name,
            memory.id = %memory_id,
        );
        let _enter = span.enter();

        self.ensure_collection().await?;

        self.vectordb
            .delete_vectors(&self.collection_name, vec![memory_id.to_string()])
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to forget memory: {}", e)))?;

        tracing::debug!(
            memory_id = %memory_id,
            collection = %self.collection_name,
            "Deleted semantic memory"
        );

        Ok(true)
    }

    /// Get a specific memory by ID
    pub async fn get(&self, memory_id: &str) -> Result<Option<MemoryResult>> {
        let span = tracing::info_span!(
            "semantic_memory.get",
            otel.name = "semantic_memory.get",
            memory.scope = %self.config.scope,
            memory.scope_id = %self.config.scope_id,
            memory.collection = %self.collection_name,
            memory.id = %memory_id,
            memory.found = tracing::field::Empty,
        );
        let _enter = span.enter();

        self.ensure_collection().await?;

        let entry = self
            .vectordb
            .get_vector(&self.collection_name, memory_id)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to get memory: {}", e)))?;

        let result = entry.and_then(|e| {
            let content = e.metadata.text?;
            Some(MemoryResult {
                id: e.id,
                content,
                score: 1.0, // Exact match
                metadata: MemoryMetadata {
                    source: e.metadata.source,
                    created_at: e
                        .metadata
                        .extra
                        .get("created_at")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    extra: e.metadata.extra,
                },
            })
        });

        span.record("memory.found", result.is_some());

        Ok(result)
    }

    /// Get the collection name being used
    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    /// Get the scope configuration
    pub fn scope(&self) -> MemoryScope {
        self.config.scope
    }

    /// Get the scope ID
    pub fn scope_id(&self) -> &str {
        &self.config.scope_id
    }

    /// Get the embedder dimension
    pub fn dimension(&self) -> u32 {
        self.embedder.dimension()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_scope_parsing() {
        assert_eq!(MemoryScope::from_str("user"), Some(MemoryScope::User));
        assert_eq!(MemoryScope::from_str("TENANT"), Some(MemoryScope::Tenant));
        assert_eq!(MemoryScope::from_str("Agent"), Some(MemoryScope::Agent));
        assert_eq!(MemoryScope::from_str("session"), Some(MemoryScope::Session));
        assert_eq!(MemoryScope::from_str("global"), Some(MemoryScope::Global));
        assert_eq!(MemoryScope::from_str("invalid"), None);
    }

    #[test]
    fn test_collection_name_generation() {
        assert_eq!(
            MemoryScope::User.collection_name("user-123"),
            "user_user_123_memories"
        );
        assert_eq!(
            MemoryScope::Tenant.collection_name("acme-corp"),
            "tenant_acme_corp_memories"
        );
        assert_eq!(
            MemoryScope::Agent.collection_name("research_agent"),
            "agent_research_agent_memories"
        );
    }

    #[test]
    fn test_memory_metadata_builder() {
        let metadata = MemoryMetadata::new()
            .with_source("conversation")
            .with_created_at("2024-01-01T00:00:00Z")
            .with_extra("priority", "high");

        assert_eq!(metadata.source, Some("conversation".to_string()));
        assert_eq!(
            metadata.created_at,
            Some("2024-01-01T00:00:00Z".to_string())
        );
        assert!(metadata.extra.contains_key("priority"));
    }

    #[test]
    fn test_config_builder() {
        let config = SemanticMemoryConfig::new(MemoryScope::User, "user-123")
            .with_distance_metric(DistanceMetric::Euclidean)
            .with_auto_create(false);

        assert_eq!(config.scope, MemoryScope::User);
        assert_eq!(config.scope_id, "user-123");
        assert_eq!(config.distance_metric, DistanceMetric::Euclidean);
        assert!(!config.auto_create_collection);
    }
}
