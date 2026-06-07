//! Semantic memory service with provider-backed vector storage.
//!
//! This is the AGNT5-native memory path. It enforces tenant/deployment/scope
//! isolation and provenance.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

#[cfg(feature = "libsql-memory")]
use crate::error::ErrorCode;
use crate::error::{Result, SdkError};
use crate::lm::Embedder;

pub mod config;
#[cfg(feature = "libsql-memory")]
pub mod providers;

pub use config::{
    MemoryFailurePolicy, MemoryProviderKind, MemoryRuntime, MemoryRuntimeConfig,
    MemoryServiceFactory,
};

/// Memory scope used for semantic recall.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    Session,
    User,
    App,
}

impl MemoryScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryScope::Session => "session",
            MemoryScope::User => "user",
            MemoryScope::App => "app",
        }
    }
}

impl std::fmt::Display for MemoryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Scope filter for retrieval. Scope and scope_id are paired to avoid leakage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryScopeFilter {
    pub scope: MemoryScope,
    pub scope_id: String,
}

impl MemoryScopeFilter {
    pub fn new(scope: MemoryScope, scope_id: impl Into<String>) -> Self {
        Self {
            scope,
            scope_id: scope_id.into(),
        }
    }
}

/// Canonical memory record payload and provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub id: String,
    pub tenant_id: String,
    pub deployment_id: String,
    pub scope: MemoryScope,
    pub scope_id: String,
    pub kind: String,
    pub content: String,
    pub metadata: JsonValue,
    pub embedding_model: String,
    pub embedding_dim: u32,
    pub source_session_id: Option<String>,
    pub source_run_id: Option<String>,
    pub source_event_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct MemoryVectorRecord {
    pub record: MemoryRecord,
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct SaveMemoryRequest {
    pub id: Option<String>,
    pub tenant_id: String,
    pub deployment_id: String,
    pub scope: MemoryScope,
    pub scope_id: String,
    pub kind: String,
    pub content: String,
    pub metadata: JsonValue,
    pub source_session_id: Option<String>,
    pub source_run_id: Option<String>,
    pub source_event_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SearchMemoryRequest {
    pub tenant_id: String,
    pub deployment_id: String,
    pub query: String,
    pub scope_filters: Vec<MemoryScopeFilter>,
    pub kinds: Vec<String>,
    pub limit: u32,
    pub min_score: Option<f32>,
}

#[derive(Debug, Clone)]
pub struct VectorSearchRequest {
    pub tenant_id: String,
    pub deployment_id: String,
    pub query_embedding: Vec<f32>,
    pub scope_filters: Vec<MemoryScopeFilter>,
    pub kinds: Vec<String>,
    pub limit: u32,
    pub min_score: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySearchResult {
    pub record: MemoryRecord,
    pub score: f32,
    pub distance: f32,
}

#[derive(Debug, Clone, Default)]
pub struct DeleteMemoryRequest {
    pub tenant_id: String,
    pub deployment_id: String,
    pub memory_id: Option<String>,
    pub scope_filter: Option<MemoryScopeFilter>,
    pub source_run_id: Option<String>,
}

/// Vector storage provider for semantic memory.
#[async_trait]
pub trait VectorMemoryProvider: Send + Sync {
    fn provider_name(&self) -> &'static str;

    async fn health_check(&self) -> Result<()>;

    async fn upsert_memory(&self, record: MemoryVectorRecord) -> Result<()>;

    async fn search_memory(&self, request: VectorSearchRequest) -> Result<Vec<MemorySearchResult>>;

    async fn delete_memory(&self, request: DeleteMemoryRequest) -> Result<u64>;
}

/// High-level memory service that embeds text and delegates storage/search.
pub struct MemoryService {
    embedder: Arc<dyn Embedder>,
    provider: Arc<dyn VectorMemoryProvider>,
}

impl MemoryService {
    pub fn new(embedder: Arc<dyn Embedder>, provider: Arc<dyn VectorMemoryProvider>) -> Self {
        Self { embedder, provider }
    }

    pub fn provider_name(&self) -> &'static str {
        self.provider.provider_name()
    }

    pub fn embedder_provider_name(&self) -> &'static str {
        self.embedder.provider_name()
    }

    pub fn embedding_model(&self) -> &str {
        self.embedder.model_name()
    }

    pub fn embedding_dimension(&self) -> u32 {
        self.embedder.dimension()
    }

    pub async fn save_memory(&self, request: SaveMemoryRequest) -> Result<MemoryRecord> {
        validate_non_empty("tenant_id", &request.tenant_id)?;
        validate_non_empty("deployment_id", &request.deployment_id)?;
        validate_non_empty("scope_id", &request.scope_id)?;
        validate_non_empty("content", &request.content)?;

        let embedding = self.embedder.embed(&request.content).await?;
        let embedding_dim = self.embedder.dimension();
        validate_embedding_dim(&embedding, embedding_dim)?;

        let now = Utc::now().to_rfc3339();
        let record = MemoryRecord {
            id: request.id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            tenant_id: request.tenant_id,
            deployment_id: request.deployment_id,
            scope: request.scope,
            scope_id: request.scope_id,
            kind: if request.kind.trim().is_empty() {
                "custom".to_string()
            } else {
                request.kind
            },
            content: request.content,
            metadata: request.metadata,
            embedding_model: self.embedder.model_name().to_string(),
            embedding_dim,
            source_session_id: request.source_session_id,
            source_run_id: request.source_run_id,
            source_event_id: request.source_event_id,
            created_at: now.clone(),
            updated_at: now,
        };

        self.provider
            .upsert_memory(MemoryVectorRecord {
                record: record.clone(),
                embedding,
            })
            .await?;

        Ok(record)
    }

    pub async fn search_memory(
        &self,
        request: SearchMemoryRequest,
    ) -> Result<Vec<MemorySearchResult>> {
        validate_non_empty("tenant_id", &request.tenant_id)?;
        validate_non_empty("deployment_id", &request.deployment_id)?;
        validate_non_empty("query", &request.query)?;
        if request.scope_filters.is_empty() {
            return Err(invalid_argument(
                "scope_filters",
                "memory search requires at least one paired scope filter",
            ));
        }
        if request.limit == 0 {
            return Ok(Vec::new());
        }

        let query_embedding = self.embedder.embed(&request.query).await?;
        validate_embedding_dim(&query_embedding, self.embedder.dimension())?;

        self.provider
            .search_memory(VectorSearchRequest {
                tenant_id: request.tenant_id,
                deployment_id: request.deployment_id,
                query_embedding,
                scope_filters: request.scope_filters,
                kinds: request.kinds,
                limit: request.limit,
                min_score: request.min_score,
            })
            .await
    }

    pub async fn delete_memory(&self, request: DeleteMemoryRequest) -> Result<u64> {
        self.provider.delete_memory(request).await
    }
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(invalid_argument(field, "must not be empty"));
    }
    Ok(())
}

fn validate_embedding_dim(embedding: &[f32], expected: u32) -> Result<()> {
    if embedding.len() != expected as usize {
        return Err(SdkError::InvalidArgument {
            message: format!(
                "embedding dimension mismatch: got {}, expected {}",
                embedding.len(),
                expected
            ),
            argument: Some("embedding".to_string()),
        });
    }
    Ok(())
}

fn invalid_argument(argument: &'static str, message: impl Into<String>) -> SdkError {
    SdkError::InvalidArgument {
        message: message.into(),
        argument: Some(argument.to_string()),
    }
}

#[cfg(feature = "libsql-memory")]
pub(crate) fn provider_error(message: impl Into<String>) -> SdkError {
    SdkError::State {
        message: message.into(),
        code: ErrorCode::InternalError,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use async_trait::async_trait;

    pub struct TestEmbedder;

    #[async_trait]
    impl Embedder for TestEmbedder {
        async fn embed(&self, text: &str) -> Result<Vec<f32>> {
            let vector = if text.contains("nearest") || text.contains("query") {
                vec![1.0, 0.0, 0.0, 0.0]
            } else if text.contains("allowed") {
                vec![0.9, 0.1, 0.0, 0.0]
            } else if text.contains("session-one") {
                vec![0.0, 1.0, 0.0, 0.0]
            } else if text.contains("session-two") {
                vec![0.0, 0.9, 0.1, 0.0]
            } else {
                vec![0.0, 0.0, 1.0, 0.0]
            };
            Ok(vector)
        }

        fn dimension(&self) -> u32 {
            4
        }

        fn provider_name(&self) -> &'static str {
            "test"
        }

        fn model_name(&self) -> &str {
            "test-embedding-4"
        }
    }
}
