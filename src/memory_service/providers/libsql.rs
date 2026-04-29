//! Embedded libSQL semantic memory provider.
//!
//! This provider intentionally uses filtered brute-force vector ranking:
//! tenant/deployment/scope filters are applied in SQL first, then the filtered
//! candidate set is ranked with `vector_distance_cos(...)`. The shared
//! `vector_top_k(...)` index path is not used because it ranks globally before
//! SQL filters and is not safe for tenant/user-isolated memory.

use std::sync::Arc;

use async_trait::async_trait;
use libsql::{params, params_from_iter, Builder, Connection, Value};
use tokio::sync::Mutex;

use crate::error::Result;
use crate::memory_service::{
    provider_error, DeleteMemoryRequest, MemoryRecord, MemoryScope, MemoryScopeFilter,
    MemorySearchResult, MemoryVectorRecord, VectorMemoryProvider, VectorSearchRequest,
};

#[derive(Debug, Clone)]
pub struct LibSqlMemoryConfig {
    pub path: String,
    pub embedding_dim: u32,
}

impl LibSqlMemoryConfig {
    pub fn new(path: impl Into<String>, embedding_dim: u32) -> Self {
        Self {
            path: path.into(),
            embedding_dim,
        }
    }

    pub fn in_memory(embedding_dim: u32) -> Self {
        Self::new(":memory:", embedding_dim)
    }
}

pub struct LibSqlMemoryProvider {
    config: LibSqlMemoryConfig,
    conn: Arc<Mutex<Connection>>,
}

impl LibSqlMemoryProvider {
    pub async fn new(config: LibSqlMemoryConfig) -> Result<Self> {
        if config.embedding_dim == 0 {
            return Err(provider_error("embedding_dim must be greater than zero"));
        }

        let db = Builder::new_local(&config.path)
            .build()
            .await
            .map_err(|e| provider_error(format!("build libSQL memory database: {e}")))?;
        let conn = db
            .connect()
            .map_err(|e| provider_error(format!("connect libSQL memory database: {e}")))?;

        let provider = Self {
            config,
            conn: Arc::new(Mutex::new(conn)),
        };
        provider.ensure_schema().await?;
        Ok(provider)
    }

    pub async fn in_memory(embedding_dim: u32) -> Result<Self> {
        Self::new(LibSqlMemoryConfig::in_memory(embedding_dim)).await
    }

    async fn ensure_schema(&self) -> Result<()> {
        let conn = self.conn.lock().await;
        let schema = format!(
            r#"
            CREATE TABLE IF NOT EXISTS memory_items (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                deployment_id TEXT NOT NULL,
                scope TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                content TEXT NOT NULL,
                metadata TEXT NOT NULL,
                embedding_model TEXT NOT NULL,
                embedding_dim INTEGER NOT NULL,
                source_session_id TEXT,
                source_run_id TEXT,
                source_event_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                embedding F32_BLOB({}) NOT NULL
            )
            "#,
            self.config.embedding_dim
        );
        conn.execute(&schema, ())
            .await
            .map_err(|e| provider_error(format!("create memory_items table: {e}")))?;
        conn.execute(
            r#"
            CREATE INDEX IF NOT EXISTS memory_items_scope_idx
            ON memory_items (
                tenant_id,
                deployment_id,
                scope,
                scope_id,
                kind,
                deleted_at
            )
            "#,
            (),
        )
        .await
        .map_err(|e| provider_error(format!("create memory scope index: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl VectorMemoryProvider for LibSqlMemoryProvider {
    fn provider_name(&self) -> &'static str {
        "libsql"
    }

    async fn health_check(&self) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.query("SELECT 1", ())
            .await
            .map_err(|e| provider_error(format!("libSQL memory health check failed: {e}")))?;
        Ok(())
    }

    async fn upsert_memory(&self, record: MemoryVectorRecord) -> Result<()> {
        if record.embedding.len() != self.config.embedding_dim as usize {
            return Err(provider_error(format!(
                "embedding dimension mismatch: got {}, expected {}",
                record.embedding.len(),
                self.config.embedding_dim
            )));
        }

        let metadata = serde_json::to_string(&record.record.metadata)
            .map_err(|e| provider_error(format!("serialize memory metadata: {e}")))?;
        let vector = vector_literal(&record.embedding);
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            INSERT INTO memory_items (
                id,
                tenant_id,
                deployment_id,
                scope,
                scope_id,
                kind,
                content,
                metadata,
                embedding_model,
                embedding_dim,
                source_session_id,
                source_run_id,
                source_event_id,
                created_at,
                updated_at,
                deleted_at,
                embedding
            )
            VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, ?15, NULL, vector32(?16)
            )
            ON CONFLICT(id) DO UPDATE SET
                tenant_id = excluded.tenant_id,
                deployment_id = excluded.deployment_id,
                scope = excluded.scope,
                scope_id = excluded.scope_id,
                kind = excluded.kind,
                content = excluded.content,
                metadata = excluded.metadata,
                embedding_model = excluded.embedding_model,
                embedding_dim = excluded.embedding_dim,
                source_session_id = excluded.source_session_id,
                source_run_id = excluded.source_run_id,
                source_event_id = excluded.source_event_id,
                updated_at = excluded.updated_at,
                deleted_at = NULL,
                embedding = excluded.embedding
            "#,
            params![
                record.record.id,
                record.record.tenant_id,
                record.record.deployment_id,
                record.record.scope.as_str(),
                record.record.scope_id,
                record.record.kind,
                record.record.content,
                metadata,
                record.record.embedding_model,
                record.record.embedding_dim as i64,
                optional_text(record.record.source_session_id),
                optional_text(record.record.source_run_id),
                optional_text(record.record.source_event_id),
                record.record.created_at,
                record.record.updated_at,
                vector,
            ],
        )
        .await
        .map_err(|e| provider_error(format!("upsert libSQL memory: {e}")))?;

        Ok(())
    }

    async fn search_memory(&self, request: VectorSearchRequest) -> Result<Vec<MemorySearchResult>> {
        if request.query_embedding.len() != self.config.embedding_dim as usize {
            return Err(provider_error(format!(
                "query embedding dimension mismatch: got {}, expected {}",
                request.query_embedding.len(),
                self.config.embedding_dim
            )));
        }
        if request.scope_filters.is_empty() || request.limit == 0 {
            return Ok(Vec::new());
        }

        let mut sql = String::from(
            r#"
            SELECT
                id,
                tenant_id,
                deployment_id,
                scope,
                scope_id,
                kind,
                content,
                metadata,
                embedding_model,
                embedding_dim,
                source_session_id,
                source_run_id,
                source_event_id,
                created_at,
                updated_at,
                vector_distance_cos(embedding, vector32(?)) AS distance
            FROM memory_items
            WHERE deleted_at IS NULL
              AND tenant_id = ?
              AND deployment_id = ?
            "#,
        );

        let mut values = vec![
            Value::Text(vector_literal(&request.query_embedding)),
            Value::Text(request.tenant_id.clone()),
            Value::Text(request.deployment_id.clone()),
        ];

        append_scope_filter_sql(&mut sql, &mut values, &request.scope_filters);
        append_kind_filter_sql(&mut sql, &mut values, &request.kinds);

        sql.push_str(
            r#"
            ORDER BY distance ASC
            LIMIT ?
            "#,
        );
        values.push(Value::Integer(request.limit as i64));

        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(&sql, params_from_iter(values))
            .await
            .map_err(|e| provider_error(format!("search libSQL memory: {e}")))?;

        let mut results = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| provider_error(format!("read libSQL memory row: {e}")))?
        {
            let distance = row
                .get::<f64>(15)
                .map_err(|e| provider_error(format!("read memory distance: {e}")))?
                as f32;
            let score = cosine_distance_to_score(distance);
            if request.min_score.is_some_and(|min| score < min) {
                continue;
            }
            results.push(MemorySearchResult {
                record: row_to_record(&row)?,
                score,
                distance,
            });
        }

        Ok(results)
    }

    async fn delete_memory(&self, request: DeleteMemoryRequest) -> Result<u64> {
        let mut sql = String::from(
            r#"
            UPDATE memory_items
            SET deleted_at = datetime('now')
            WHERE deleted_at IS NULL
              AND tenant_id = ?
              AND deployment_id = ?
            "#,
        );
        let mut values = vec![
            Value::Text(request.tenant_id),
            Value::Text(request.deployment_id),
        ];

        if let Some(memory_id) = request.memory_id {
            sql.push_str(" AND id = ?");
            values.push(Value::Text(memory_id));
        }
        if let Some(scope_filter) = request.scope_filter {
            sql.push_str(" AND scope = ? AND scope_id = ?");
            values.push(Value::Text(scope_filter.scope.as_str().to_string()));
            values.push(Value::Text(scope_filter.scope_id));
        }
        if let Some(source_run_id) = request.source_run_id {
            sql.push_str(" AND source_run_id = ?");
            values.push(Value::Text(source_run_id));
        }

        let conn = self.conn.lock().await;
        let changed = conn
            .execute(&sql, params_from_iter(values))
            .await
            .map_err(|e| provider_error(format!("delete libSQL memory: {e}")))?;
        Ok(changed)
    }
}

fn append_scope_filter_sql(
    sql: &mut String,
    values: &mut Vec<Value>,
    scope_filters: &[MemoryScopeFilter],
) {
    sql.push_str(" AND (");
    for (idx, filter) in scope_filters.iter().enumerate() {
        if idx > 0 {
            sql.push_str(" OR ");
        }
        sql.push_str("(scope = ? AND scope_id = ?)");
        values.push(Value::Text(filter.scope.as_str().to_string()));
        values.push(Value::Text(filter.scope_id.clone()));
    }
    sql.push(')');
}

fn append_kind_filter_sql(sql: &mut String, values: &mut Vec<Value>, kinds: &[String]) {
    if kinds.is_empty() {
        return;
    }
    sql.push_str(" AND kind IN (");
    for (idx, kind) in kinds.iter().enumerate() {
        if idx > 0 {
            sql.push_str(", ");
        }
        sql.push('?');
        values.push(Value::Text(kind.clone()));
    }
    sql.push(')');
}

fn row_to_record(row: &libsql::Row) -> Result<MemoryRecord> {
    let scope: String = row
        .get(3)
        .map_err(|e| provider_error(format!("read memory scope: {e}")))?;
    let metadata_json: String = row
        .get(7)
        .map_err(|e| provider_error(format!("read memory metadata: {e}")))?;
    let metadata = serde_json::from_str(&metadata_json)
        .map_err(|e| provider_error(format!("parse memory metadata: {e}")))?;

    Ok(MemoryRecord {
        id: row
            .get(0)
            .map_err(|e| provider_error(format!("read memory id: {e}")))?,
        tenant_id: row
            .get(1)
            .map_err(|e| provider_error(format!("read memory tenant_id: {e}")))?,
        deployment_id: row
            .get(2)
            .map_err(|e| provider_error(format!("read memory deployment_id: {e}")))?,
        scope: parse_scope(&scope)?,
        scope_id: row
            .get(4)
            .map_err(|e| provider_error(format!("read memory scope_id: {e}")))?,
        kind: row
            .get(5)
            .map_err(|e| provider_error(format!("read memory kind: {e}")))?,
        content: row
            .get(6)
            .map_err(|e| provider_error(format!("read memory content: {e}")))?,
        metadata,
        embedding_model: row
            .get(8)
            .map_err(|e| provider_error(format!("read memory embedding_model: {e}")))?,
        embedding_dim: row
            .get::<i64>(9)
            .map_err(|e| provider_error(format!("read memory embedding_dim: {e}")))?
            as u32,
        source_session_id: row
            .get(10)
            .map_err(|e| provider_error(format!("read memory source_session_id: {e}")))?,
        source_run_id: row
            .get(11)
            .map_err(|e| provider_error(format!("read memory source_run_id: {e}")))?,
        source_event_id: row
            .get(12)
            .map_err(|e| provider_error(format!("read memory source_event_id: {e}")))?,
        created_at: row
            .get(13)
            .map_err(|e| provider_error(format!("read memory created_at: {e}")))?,
        updated_at: row
            .get(14)
            .map_err(|e| provider_error(format!("read memory updated_at: {e}")))?,
    })
}

fn parse_scope(scope: &str) -> Result<MemoryScope> {
    match scope {
        "session" => Ok(MemoryScope::Session),
        "user" => Ok(MemoryScope::User),
        "app" => Ok(MemoryScope::App),
        other => Err(provider_error(format!("unknown memory scope: {other}"))),
    }
}

fn optional_text(value: Option<String>) -> Value {
    value.map(Value::Text).unwrap_or(Value::Null)
}

fn vector_literal(vector: &[f32]) -> String {
    let mut out = String::from("[");
    for (idx, value) in vector.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    out
}

fn cosine_distance_to_score(distance: f32) -> f32 {
    (1.0 - distance).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::memory_service::tests::TestEmbedder;
    use crate::memory_service::{MemoryService, SaveMemoryRequest, SearchMemoryRequest};

    async fn service() -> Result<MemoryService> {
        let provider = Arc::new(LibSqlMemoryProvider::in_memory(4).await?);
        let embedder = Arc::new(TestEmbedder);
        Ok(MemoryService::new(embedder, provider))
    }

    fn save_request(
        tenant_id: &str,
        deployment_id: &str,
        scope: MemoryScope,
        scope_id: &str,
        content: &str,
    ) -> SaveMemoryRequest {
        SaveMemoryRequest {
            id: None,
            tenant_id: tenant_id.to_string(),
            deployment_id: deployment_id.to_string(),
            scope,
            scope_id: scope_id.to_string(),
            kind: "preference".to_string(),
            content: content.to_string(),
            metadata: json!({}),
            source_session_id: None,
            source_run_id: None,
            source_event_id: None,
        }
    }

    #[tokio::test]
    async fn user_memory_is_retrievable_across_sessions() -> Result<()> {
        let service = service().await?;
        service
            .save_memory(SaveMemoryRequest {
                source_session_id: Some("session-a".to_string()),
                ..save_request(
                    "tenant-a",
                    "dep-1",
                    MemoryScope::User,
                    "user-a",
                    "allowed user memory",
                )
            })
            .await?;

        let hits = service
            .search_memory(SearchMemoryRequest {
                tenant_id: "tenant-a".to_string(),
                deployment_id: "dep-1".to_string(),
                query: "query".to_string(),
                scope_filters: vec![MemoryScopeFilter::new(MemoryScope::User, "user-a")],
                kinds: vec![],
                limit: 5,
                min_score: None,
            })
            .await?;

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record.scope_id, "user-a");
        assert_eq!(
            hits[0].record.source_session_id.as_deref(),
            Some("session-a")
        );
        Ok(())
    }

    #[tokio::test]
    async fn session_memory_is_isolated_by_scope_id() -> Result<()> {
        let service = service().await?;
        service
            .save_memory(save_request(
                "tenant-a",
                "dep-1",
                MemoryScope::Session,
                "session-1",
                "session-one memory",
            ))
            .await?;
        service
            .save_memory(save_request(
                "tenant-a",
                "dep-1",
                MemoryScope::Session,
                "session-2",
                "session-two memory",
            ))
            .await?;

        let hits = service
            .search_memory(SearchMemoryRequest {
                tenant_id: "tenant-a".to_string(),
                deployment_id: "dep-1".to_string(),
                query: "session-one query".to_string(),
                scope_filters: vec![MemoryScopeFilter::new(MemoryScope::Session, "session-1")],
                kinds: vec![],
                limit: 5,
                min_score: None,
            })
            .await?;

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record.scope_id, "session-1");
        assert!(hits[0].record.content.contains("session-one"));
        Ok(())
    }

    #[tokio::test]
    async fn tenant_filter_is_applied_before_vector_ranking() -> Result<()> {
        let service = service().await?;
        service
            .save_memory(save_request(
                "tenant-b",
                "dep-1",
                MemoryScope::User,
                "user-b",
                "nearest forbidden memory",
            ))
            .await?;
        service
            .save_memory(save_request(
                "tenant-a",
                "dep-1",
                MemoryScope::User,
                "user-a",
                "allowed user memory",
            ))
            .await?;

        let hits = service
            .search_memory(SearchMemoryRequest {
                tenant_id: "tenant-a".to_string(),
                deployment_id: "dep-1".to_string(),
                query: "query".to_string(),
                scope_filters: vec![MemoryScopeFilter::new(MemoryScope::User, "user-a")],
                kinds: vec![],
                limit: 1,
                min_score: None,
            })
            .await?;

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record.tenant_id, "tenant-a");
        assert_eq!(hits[0].record.scope_id, "user-a");
        assert!(hits[0].record.content.contains("allowed"));
        Ok(())
    }

    #[tokio::test]
    async fn file_backed_memory_persists_across_restart() -> Result<()> {
        let path = std::env::temp_dir().join(format!("agnt5-memory-{}.db", uuid::Uuid::new_v4()));
        let path_string = path.to_string_lossy().to_string();

        {
            let provider = Arc::new(
                LibSqlMemoryProvider::new(LibSqlMemoryConfig::new(&path_string, 4)).await?,
            );
            let service = MemoryService::new(Arc::new(TestEmbedder), provider);
            service
                .save_memory(save_request(
                    "tenant-a",
                    "dep-1",
                    MemoryScope::User,
                    "user-a",
                    "allowed persisted memory",
                ))
                .await?;
        }

        {
            let provider = Arc::new(
                LibSqlMemoryProvider::new(LibSqlMemoryConfig::new(&path_string, 4)).await?,
            );
            let service = MemoryService::new(Arc::new(TestEmbedder), provider);
            let hits = service
                .search_memory(SearchMemoryRequest {
                    tenant_id: "tenant-a".to_string(),
                    deployment_id: "dep-1".to_string(),
                    query: "query".to_string(),
                    scope_filters: vec![MemoryScopeFilter::new(MemoryScope::User, "user-a")],
                    kinds: vec![],
                    limit: 5,
                    min_score: None,
                })
                .await?;
            assert_eq!(hits.len(), 1);
            assert!(hits[0].record.content.contains("persisted"));
        }

        let _ = std::fs::remove_file(path);
        Ok(())
    }
}
