// PostgreSQL pgvector provider implementation
// Uses sqlx with pgvector extension for vector similarity search

use async_trait::async_trait;

use super::super::{
    Collection, CollectionInfo, DistanceMetric, SearchQuery, SearchResult, VectorDatabase,
    VectorEntry, VectorFilter, VectorMetadata,
};
use crate::error::{Result, SdkError};

#[cfg(feature = "pgvector")]
use pgvector::Vector;
#[cfg(feature = "pgvector")]
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

/// PostgreSQL pgvector provider
///
/// Connects to user's PostgreSQL database with pgvector extension.
/// Requires:
/// - PostgreSQL with pgvector extension installed
/// - Environment variable: POSTGRES_URL or DATABASE_URL
///
/// Each collection is stored as a separate table with schema:
/// - id: TEXT PRIMARY KEY
/// - vector: VECTOR(dimension)
/// - metadata: JSONB
pub struct PgVectorProvider {
    #[cfg(feature = "pgvector")]
    pool: PgPool,
    #[cfg(not(feature = "pgvector"))]
    #[allow(dead_code)]
    database_url: String,
}

impl PgVectorProvider {
    /// Create a new pgvector provider
    #[cfg(feature = "pgvector")]
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))
            })?;

        // Ensure pgvector extension is available
        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(&pool)
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!(
                    "Failed to enable pgvector extension: {}. \
                     Make sure pgvector is installed in your PostgreSQL database.",
                    e
                ))
            })?;

        Ok(Self { pool })
    }

    #[cfg(not(feature = "pgvector"))]
    pub async fn new(_database_url: &str) -> Result<Self> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled. \
             Rebuild with `cargo build --features pgvector` to enable PostgreSQL pgvector support."
        )))
    }

    /// Create provider from environment variables
    pub async fn from_env() -> Result<Self> {
        let database_url = std::env::var("POSTGRES_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .map_err(|_| {
                SdkError::Other(anyhow::anyhow!(
                    "Neither POSTGRES_URL nor DATABASE_URL environment variable is set"
                ))
            })?;

        Self::new(&database_url).await
    }

    /// Get the table name for a collection (sanitized)
    fn table_name(collection_name: &str) -> String {
        // Sanitize collection name to prevent SQL injection
        let sanitized: String = collection_name
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        format!("vectors_{}", sanitized)
    }

    #[cfg(feature = "pgvector")]
    fn distance_to_operator(metric: DistanceMetric) -> &'static str {
        match metric {
            DistanceMetric::Cosine => "<=>",     // Cosine distance
            DistanceMetric::Euclidean => "<->",  // L2 distance
            DistanceMetric::DotProduct => "<#>", // Negative inner product
            DistanceMetric::Manhattan => "<+>",  // L1 distance (taxicab)
        }
    }
}

#[async_trait]
impl VectorDatabase for PgVectorProvider {
    fn provider_name(&self) -> &'static str {
        "pgvector"
    }

    #[cfg(feature = "pgvector")]
    async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("PostgreSQL health check failed: {}", e)))?;
        Ok(())
    }

    #[cfg(not(feature = "pgvector"))]
    async fn health_check(&self) -> Result<()> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn create_collection(&self, collection: &Collection) -> Result<()> {
        let table = Self::table_name(&collection.name);

        // Create table with vector column
        let create_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                vector VECTOR({}),
                metadata JSONB DEFAULT '{{}}',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
            table, collection.dimension
        );

        sqlx::query(&create_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to create collection: {}", e)))?;

        // Create index for vector similarity search
        // Use IVFFlat or HNSW depending on expected size
        let index_sql = format!(
            r#"
            CREATE INDEX IF NOT EXISTS {}_vector_idx ON {}
            USING hnsw (vector vector_cosine_ops)
            "#,
            table, table
        );

        sqlx::query(&index_sql).execute(&self.pool).await.map_err(|e| {
            // Index creation may fail if table is empty, which is fine
            tracing::debug!("Index creation note: {}", e);
        }).ok();

        // Store collection metadata
        let meta_sql = r#"
            CREATE TABLE IF NOT EXISTS vector_collections (
                name TEXT PRIMARY KEY,
                dimension INTEGER NOT NULL,
                distance_metric TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        "#;

        sqlx::query(meta_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Failed to create metadata table: {}", e))
            })?;

        let upsert_meta = r#"
            INSERT INTO vector_collections (name, dimension, distance_metric, description)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name) DO UPDATE SET
                dimension = EXCLUDED.dimension,
                distance_metric = EXCLUDED.distance_metric,
                description = EXCLUDED.description
        "#;

        sqlx::query(upsert_meta)
            .bind(&collection.name)
            .bind(collection.dimension as i32)
            .bind(collection.distance_metric.to_string())
            .bind(&collection.description)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Failed to save collection metadata: {}", e))
            })?;

        Ok(())
    }

    #[cfg(not(feature = "pgvector"))]
    async fn create_collection(&self, _collection: &Collection) -> Result<()> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn delete_collection(&self, name: &str) -> Result<()> {
        let table = Self::table_name(name);

        let drop_sql = format!("DROP TABLE IF EXISTS {}", table);
        sqlx::query(&drop_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to delete collection: {}", e)))?;

        // Remove from metadata
        sqlx::query("DELETE FROM vector_collections WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .ok(); // Ignore errors if metadata table doesn't exist

        Ok(())
    }

    #[cfg(not(feature = "pgvector"))]
    async fn delete_collection(&self, _name: &str) -> Result<()> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn list_collections(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT name FROM vector_collections ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to list collections: {}", e)))?;

        let names: Vec<String> = rows.iter().map(|row| row.get("name")).collect();
        Ok(names)
    }

    #[cfg(not(feature = "pgvector"))]
    async fn list_collections(&self) -> Result<Vec<String>> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn upsert_vectors(&self, collection_name: &str, vectors: Vec<VectorEntry>) -> Result<()> {
        let table = Self::table_name(collection_name);

        for entry in vectors {
            let vector = Vector::from(entry.vector);
            let metadata = serde_json::to_value(&entry.metadata).unwrap_or_default();

            let sql = format!(
                r#"
                INSERT INTO {} (id, vector, metadata)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET
                    vector = EXCLUDED.vector,
                    metadata = EXCLUDED.metadata
                "#,
                table
            );

            sqlx::query(&sql)
                .bind(&entry.id)
                .bind(&vector)
                .bind(&metadata)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SdkError::Other(anyhow::anyhow!("Failed to upsert vector {}: {}", entry.id, e))
                })?;
        }

        Ok(())
    }

    #[cfg(not(feature = "pgvector"))]
    async fn upsert_vectors(
        &self,
        _collection_name: &str,
        _vectors: Vec<VectorEntry>,
    ) -> Result<()> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn search_vectors(
        &self,
        collection_name: &str,
        query: SearchQuery,
    ) -> Result<Vec<SearchResult>> {
        let table = Self::table_name(collection_name);
        let operator = Self::distance_to_operator(
            query.distance_metric.unwrap_or(DistanceMetric::Cosine),
        );

        let query_vector = Vector::from(query.vector);

        // Build the query based on what fields we need
        let sql = format!(
            r#"
            SELECT
                id,
                vector {} $1 AS distance,
                1 - (vector {} $1) AS score,
                {}
                {}
            FROM {}
            ORDER BY vector {} $1
            LIMIT $2
            "#,
            operator,
            operator,
            if query.include_metadata {
                "metadata,"
            } else {
                ""
            },
            if query.include_vectors {
                "vector"
            } else {
                "NULL::vector AS vector"
            },
            table,
            operator
        );

        let rows = sqlx::query(&sql)
            .bind(&query_vector)
            .bind(query.limit as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to search vectors: {}", e)))?;

        let results: Vec<SearchResult> = rows
            .iter()
            .filter_map(|row| {
                let distance: f32 = row.get("distance");
                let score: f32 = row.get("score");

                // Apply min_score filter
                if let Some(min_score) = query.min_score {
                    if score < min_score {
                        return None;
                    }
                }

                let metadata = if query.include_metadata {
                    let meta_json: serde_json::Value = row.get("metadata");
                    serde_json::from_value(meta_json).ok()
                } else {
                    None
                };

                let vector = if query.include_vectors {
                    let v: Option<Vector> = row.get("vector");
                    v.map(|v| v.to_vec())
                } else {
                    None
                };

                Some(SearchResult {
                    id: row.get("id"),
                    score,
                    distance,
                    vector,
                    metadata,
                })
            })
            .collect();

        Ok(results)
    }

    #[cfg(not(feature = "pgvector"))]
    async fn search_vectors(
        &self,
        _collection_name: &str,
        _query: SearchQuery,
    ) -> Result<Vec<SearchResult>> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn delete_vectors(&self, collection_name: &str, ids: Vec<String>) -> Result<()> {
        let table = Self::table_name(collection_name);

        // Use ANY for batch delete
        let sql = format!("DELETE FROM {} WHERE id = ANY($1)", table);

        sqlx::query(&sql)
            .bind(&ids)
            .execute(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to delete vectors: {}", e)))?;

        Ok(())
    }

    #[cfg(not(feature = "pgvector"))]
    async fn delete_vectors(&self, _collection_name: &str, _ids: Vec<String>) -> Result<()> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn delete_by_filter(&self, collection_name: &str, filter: VectorFilter) -> Result<()> {
        let table = Self::table_name(collection_name);

        // Build WHERE clause from filter
        let mut conditions = Vec::new();
        let mut params: Vec<String> = Vec::new();

        for condition in &filter.must {
            let param_idx = params.len() + 1;
            let value_str = match &condition.value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            conditions.push(format!(
                "metadata->>'{}' = ${}",
                condition.field, param_idx
            ));
            params.push(value_str);
        }

        if conditions.is_empty() {
            return Ok(());
        }

        let sql = format!(
            "DELETE FROM {} WHERE {}",
            table,
            conditions.join(" AND ")
        );

        let mut query = sqlx::query(&sql);
        for param in &params {
            query = query.bind(param);
        }

        query.execute(&self.pool).await.map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to delete vectors by filter: {}", e))
        })?;

        Ok(())
    }

    #[cfg(not(feature = "pgvector"))]
    async fn delete_by_filter(&self, _collection_name: &str, _filter: VectorFilter) -> Result<()> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn get_vector(&self, collection_name: &str, id: &str) -> Result<Option<VectorEntry>> {
        let table = Self::table_name(collection_name);

        let sql = format!(
            "SELECT id, vector, metadata FROM {} WHERE id = $1",
            table
        );

        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to get vector: {}", e)))?;

        Ok(row.map(|row| {
            let vector: Vector = row.get("vector");
            let metadata: serde_json::Value = row.get("metadata");

            VectorEntry {
                id: row.get("id"),
                vector: vector.to_vec(),
                metadata: serde_json::from_value(metadata).unwrap_or_default(),
            }
        }))
    }

    #[cfg(not(feature = "pgvector"))]
    async fn get_vector(&self, _collection_name: &str, _id: &str) -> Result<Option<VectorEntry>> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }

    #[cfg(feature = "pgvector")]
    async fn collection_info(&self, collection_name: &str) -> Result<CollectionInfo> {
        let table = Self::table_name(collection_name);

        // Get count from table
        let count_sql = format!("SELECT COUNT(*) as count FROM {}", table);
        let count_row = sqlx::query(&count_sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to get collection info: {}", e)))?;

        let vector_count: i64 = count_row.get("count");

        // Get metadata from collections table
        let meta_row = sqlx::query(
            "SELECT dimension, distance_metric FROM vector_collections WHERE name = $1",
        )
        .bind(collection_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to get collection metadata: {}", e))
        })?;

        let (dimension, distance_metric) = meta_row
            .map(|row| {
                let dim: i32 = row.get("dimension");
                let metric: String = row.get("distance_metric");
                let dm = match metric.as_str() {
                    "euclidean" => DistanceMetric::Euclidean,
                    "dot_product" => DistanceMetric::DotProduct,
                    "manhattan" => DistanceMetric::Manhattan,
                    _ => DistanceMetric::Cosine,
                };
                (dim as u32, dm)
            })
            .unwrap_or((1536, DistanceMetric::Cosine));

        Ok(CollectionInfo {
            name: collection_name.to_string(),
            vector_count: vector_count as u64,
            indexed_vector_count: vector_count as u64,
            points_count: vector_count as u64,
            segments_count: 1,
            status: "ready".to_string(),
            dimension,
            distance_metric,
        })
    }

    #[cfg(not(feature = "pgvector"))]
    async fn collection_info(&self, _collection_name: &str) -> Result<CollectionInfo> {
        Err(SdkError::Other(anyhow::anyhow!(
            "pgvector feature is not enabled"
        )))
    }
}

#[cfg(all(test, feature = "pgvector"))]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_sanitization() {
        assert_eq!(
            PgVectorProvider::table_name("my_collection"),
            "vectors_my_collection"
        );
        assert_eq!(
            PgVectorProvider::table_name("test-collection!@#"),
            "vectors_testcollection"
        );
        assert_eq!(
            PgVectorProvider::table_name("user_123_memories"),
            "vectors_user_123_memories"
        );
    }
}
