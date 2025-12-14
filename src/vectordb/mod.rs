// Vector database integration for AGNT5 SDK-Core
// Provides unified interface for vector storage and retrieval operations

pub mod providers;
pub mod types;

// Re-export core types
pub use providers::{
    agnt5::{Agnt5Provider, Agnt5ProviderConfig},
    pgvector::PgVectorProvider,
    qdrant::QdrantProvider,
};
pub use types::{
    Collection, DistanceMetric, SearchQuery, SearchResult, VectorEntry, VectorFilter,
    VectorMetadata,
};

use crate::error::Result;
use async_trait::async_trait;

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
    async fn upsert_vectors(&self, collection_name: &str, vectors: Vec<VectorEntry>) -> Result<()>;

    /// Search for similar vectors
    async fn search_vectors(
        &self,
        collection_name: &str,
        query: SearchQuery,
    ) -> Result<Vec<SearchResult>>;

    /// Delete vectors by IDs
    async fn delete_vectors(&self, collection_name: &str, ids: Vec<String>) -> Result<()>;

    /// Delete vectors by filter
    async fn delete_by_filter(&self, collection_name: &str, filter: VectorFilter) -> Result<()>;

    /// Get vector by ID
    async fn get_vector(&self, collection_name: &str, id: &str) -> Result<Option<VectorEntry>>;

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
    pub fn register_provider(
        &mut self,
        name: String,
        provider: std::sync::Arc<dyn VectorDatabase>,
    ) {
        tracing::info!(
            "Registering vector database provider: {} (type: {})",
            name,
            provider.provider_name()
        );
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
            Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Vector database provider not found: {}",
                name
            )))
        }
    }

    /// Get the default provider
    pub fn get_default_provider(&self) -> Option<std::sync::Arc<dyn VectorDatabase>> {
        self.default_provider
            .as_ref()
            .and_then(|name| self.get_provider(name))
    }

    /// List all registered provider names
    pub fn list_providers(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    /// Load providers from environment variables
    /// Detection order:
    /// 1. AGNT5 Platform (default) - uses platform's backend (SQLite/PostgreSQL/CockroachDB+Qdrant)
    /// 2. QDRANT_URL - direct connection to user's Qdrant
    /// 3. POSTGRES_URL - direct connection to user's PostgreSQL with pgvector
    pub async fn load_from_environment(&mut self) -> Result<()> {
        let mut loaded_count = 0;

        // AGNT5 Platform (default provider)
        // Always available - uses platform gateway for vector operations
        match Agnt5Provider::from_env() {
            Ok(provider) => {
                self.register_provider("agnt5".to_string(), std::sync::Arc::new(provider));
                loaded_count += 1;

                // AGNT5 is the default provider
                self.default_provider = Some("agnt5".to_string());
            }
            Err(e) => {
                tracing::debug!("AGNT5 provider not available: {}", e);
            }
        }

        // Qdrant (user's own instance)
        if let Ok(url) = std::env::var("QDRANT_URL") {
            match QdrantProvider::new(&url, None).await {
                Ok(provider) => {
                    self.register_provider("qdrant".to_string(), std::sync::Arc::new(provider));
                    loaded_count += 1;

                    // If no default yet, use Qdrant
                    if self.default_provider.is_none() {
                        self.default_provider = Some("qdrant".to_string());
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to Qdrant at {}: {}", url, e);
                }
            }
        }

        // pgvector (user's own PostgreSQL)
        if let Ok(database_url) =
            std::env::var("POSTGRES_URL").or_else(|_| std::env::var("DATABASE_URL"))
        {
            match PgVectorProvider::new(&database_url).await {
                Ok(provider) => {
                    self.register_provider("pgvector".to_string(), std::sync::Arc::new(provider));
                    loaded_count += 1;

                    if self.default_provider.is_none() {
                        self.default_provider = Some("pgvector".to_string());
                    }
                }
                Err(e) => {
                    tracing::debug!("pgvector provider not available: {}", e);
                }
            }
        }

        if loaded_count == 0 {
            tracing::warn!(
                "No vector database providers loaded. AGNT5 platform provider will be used by default."
            );
            // Try to create AGNT5 provider with defaults even if from_env failed
            let config = Agnt5ProviderConfig::new("http://localhost:34183");
            if let Ok(provider) = Agnt5Provider::new(config) {
                self.register_provider("agnt5".to_string(), std::sync::Arc::new(provider));
                self.default_provider = Some("agnt5".to_string());
                loaded_count = 1;
            }
        }

        if loaded_count == 0 {
            return Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "No vector database providers available"
            )));
        }

        tracing::info!(
            "Loaded {} vector database providers from environment (default: {:?})",
            loaded_count,
            self.default_provider
        );

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
