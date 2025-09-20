// Vector database integration for AGNT5 SDK-Core
// Provides unified interface for vector storage and retrieval operations

pub mod providers;
pub mod types;
pub mod rag;

// Re-export core types
pub use types::{
    VectorEntry, VectorMetadata, SearchQuery, SearchResult,
    DistanceMetric, VectorFilter, Collection
};
pub use providers::{qdrant::QdrantProvider, pgvector::PgVectorProvider};

use async_trait::async_trait;
use crate::error::Result;
use serde_json::Value;

/// Core trait that all vector database providers must implement
#[async_trait]
pub trait VectorDatabase: Send + Sync {
    /// Get the provider's unique identifier
    fn provider_name(&self) -> &'static str;

    /// Check if the vector database is healthy and accessible
    async fn health_check(&self) -> Result<()>;

    /// Create a new collection with specified configuration
    async fn create_collection(&self, collection: &Collection) -> Result<()>;

    /// Delete a collection
    async fn delete_collection(&self, name: &str) -> Result<()>;

    /// List all collections
    async fn list_collections(&self) -> Result<Vec<String>>;

    /// Insert or update vectors in a collection
    async fn upsert_vectors(
        &self,
        collection_name: &str,
        vectors: Vec<VectorEntry>,
    ) -> Result<()>;

    /// Search for similar vectors
    async fn search_vectors(
        &self,
        collection_name: &str,
        query: SearchQuery,
    ) -> Result<Vec<SearchResult>>;

    /// Delete vectors by IDs
    async fn delete_vectors(
        &self,
        collection_name: &str,
        ids: Vec<String>,
    ) -> Result<()>;

    /// Delete vectors by filter
    async fn delete_by_filter(
        &self,
        collection_name: &str,
        filter: VectorFilter,
    ) -> Result<()>;

    /// Get vector by ID
    async fn get_vector(
        &self,
        collection_name: &str,
        id: &str,
    ) -> Result<Option<VectorEntry>>;

    /// Get collection statistics
    async fn collection_info(&self, collection_name: &str) -> Result<CollectionInfo>;
}

/// Information about a vector collection
#[derive(Debug, Clone)]
pub struct CollectionInfo {
    pub name: String,
    pub vector_count: u64,
    pub indexed_vector_count: u64,
    pub points_count: u64,
    pub segments_count: u32,
    pub status: String,
    pub dimension: u32,
    pub distance_metric: DistanceMetric,
}

/// Registry for managing multiple vector database providers
pub struct VectorDbRegistry {
    providers: std::collections::HashMap<String, std::sync::Arc<dyn VectorDatabase>>,
    default_provider: Option<String>,
}

impl VectorDbRegistry {
    pub fn new() -> Self {
        Self {
            providers: std::collections::HashMap::new(),
            default_provider: None,
        }
    }

    /// Register a vector database provider
    pub fn register_provider(&mut self, name: String, provider: std::sync::Arc<dyn VectorDatabase>) {
        tracing::info!("Registering vector database provider: {} (type: {})", name, provider.provider_name());
        self.providers.insert(name, provider);
    }

    /// Get a provider by name
    pub fn get_provider(&self, name: &str) -> Option<std::sync::Arc<dyn VectorDatabase>> {
        self.providers.get(name).cloned()
    }

    /// Set the default provider
    pub fn set_default_provider(&mut self, name: String) -> Result<()> {
        if self.providers.contains_key(&name) {
            self.default_provider = Some(name);
            Ok(())
        } else {
            Err(crate::error::SdkError::Other(anyhow::anyhow!("Vector database provider not found: {}", name)))
        }
    }

    /// Get the default provider
    pub fn get_default_provider(&self) -> Option<std::sync::Arc<dyn VectorDatabase>> {
        self.default_provider.as_ref()
            .and_then(|name| self.get_provider(name))
    }

    /// List all registered provider names
    pub fn list_providers(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    /// Load providers from environment variables
    pub async fn load_from_environment(&mut self) -> Result<()> {
        let mut loaded_count = 0;

        // Qdrant
        if let Ok(url) = std::env::var("QDRANT_URL") {
            let provider = QdrantProvider::new(&url, None).await?;
            self.register_provider("qdrant".to_string(), std::sync::Arc::new(provider));
            loaded_count += 1;

            if self.default_provider.is_none() {
                self.default_provider = Some("qdrant".to_string());
            }
        }

        // pgvector (PostgreSQL)
        if let Ok(database_url) = std::env::var("POSTGRES_URL").or_else(|_| std::env::var("DATABASE_URL")) {
            let provider = PgVectorProvider::new(&database_url).await?;
            self.register_provider("pgvector".to_string(), std::sync::Arc::new(provider));
            loaded_count += 1;

            if self.default_provider.is_none() {
                self.default_provider = Some("pgvector".to_string());
            }
        }

        if loaded_count == 0 {
            tracing::warn!("No vector database providers loaded from environment. Set QDRANT_URL or POSTGRES_URL.");
            return Err(crate::error::SdkError::Other(anyhow::anyhow!("No vector database providers available")));
        } else {
            tracing::info!("Loaded {} vector database providers from environment", loaded_count);
        }

        Ok(())
    }

    /// Check health of all providers
    pub async fn health_check(&self) -> std::collections::HashMap<String, Result<()>> {
        let mut results = std::collections::HashMap::new();

        for (name, provider) in &self.providers {
            let result = provider.health_check().await;
            results.insert(name.clone(), result);
        }

        results
    }
}

impl Default for VectorDbRegistry {
    fn default() -> Self {
        Self::new()
    }
}