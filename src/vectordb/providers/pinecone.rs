// Pinecone vector database provider implementation
// Uses Pinecone's REST API: https://docs.pinecone.io/reference/api/introduction

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::super::{
    Collection, CollectionInfo, DistanceMetric, SearchQuery, SearchResult, VectorDatabase,
    VectorEntry, VectorFilter, VectorMetadata,
};
use crate::error::{Result, SdkError};

/// Pinecone vector database provider
///
/// Connects to user's Pinecone index via REST API.
/// Requires environment variables:
/// - PINECONE_API_KEY: API key for authentication
/// - PINECONE_HOST: Full index host URL (e.g., https://my-index-abc123.svc.us-east-1.pinecone.io)
pub struct PineconeProvider {
    client: Client,
    host: String,
    api_key: String,
}

// Pinecone API request/response types
#[derive(Serialize)]
struct UpsertRequest {
    vectors: Vec<PineconeVector>,
    namespace: Option<String>,
}

#[derive(Serialize)]
struct PineconeVector {
    id: String,
    values: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
struct QueryRequest {
    vector: Vec<f32>,
    #[serde(rename = "topK")]
    top_k: u32,
    #[serde(rename = "includeMetadata")]
    include_metadata: bool,
    #[serde(rename = "includeValues")]
    include_values: bool,
    namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Deserialize)]
struct QueryResponse {
    matches: Vec<QueryMatch>,
    #[allow(dead_code)]
    namespace: Option<String>,
}

#[derive(Deserialize)]
struct QueryMatch {
    id: String,
    score: f32,
    #[serde(default)]
    values: Option<Vec<f32>>,
    #[serde(default)]
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
struct DeleteRequest {
    ids: Option<Vec<String>>,
    #[serde(rename = "deleteAll")]
    delete_all: Option<bool>,
    namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
#[allow(dead_code)]
struct FetchRequest {
    ids: Vec<String>,
    namespace: Option<String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct FetchResponse {
    vectors: HashMap<String, FetchedVector>,
    namespace: Option<String>,
}

#[derive(Deserialize)]
struct FetchedVector {
    id: String,
    values: Vec<f32>,
    #[serde(default)]
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct DescribeIndexStatsResponse {
    namespaces: Option<HashMap<String, NamespaceStats>>,
    dimension: u32,
    #[serde(rename = "indexFullness")]
    index_fullness: f32,
    #[serde(rename = "totalVectorCount")]
    total_vector_count: u64,
}

#[derive(Deserialize)]
struct NamespaceStats {
    #[serde(rename = "vectorCount")]
    vector_count: u64,
}

#[derive(Deserialize)]
struct PineconeError {
    message: String,
    #[serde(default)]
    #[allow(dead_code)]
    code: Option<u32>,
}

impl PineconeProvider {
    /// Create a new Pinecone provider with explicit configuration
    pub fn new(host: &str, api_key: &str) -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to create HTTP client: {}", e)))?;

        // Normalize host URL (remove trailing slash)
        let host = host.trim_end_matches('/').to_string();

        Ok(Self {
            client,
            host,
            api_key: api_key.to_string(),
        })
    }

    /// Create a Pinecone provider from environment variables
    ///
    /// Required:
    /// - PINECONE_API_KEY: API key
    /// - PINECONE_HOST: Full index host URL
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("PINECONE_API_KEY").map_err(|_| {
            SdkError::Other(anyhow::anyhow!(
                "PINECONE_API_KEY environment variable not set"
            ))
        })?;

        let host = std::env::var("PINECONE_HOST").map_err(|_| {
            SdkError::Other(anyhow::anyhow!(
                "PINECONE_HOST environment variable not set. \
                 Set to your index host URL, e.g., https://my-index-abc123.svc.us-east-1.pinecone.io"
            ))
        })?;

        Self::new(&host, &api_key)
    }

    /// Build the full URL for an API endpoint
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.host, path)
    }

    /// Convert VectorMetadata to Pinecone metadata format
    fn metadata_to_pinecone(metadata: &VectorMetadata) -> HashMap<String, serde_json::Value> {
        let mut result = HashMap::new();

        if let Some(text) = &metadata.text {
            result.insert("text".to_string(), serde_json::Value::String(text.clone()));
        }

        if let Some(source) = &metadata.source {
            result.insert(
                "source".to_string(),
                serde_json::Value::String(source.clone()),
            );
        }

        if let Some(chunk_index) = metadata.chunk_index {
            result.insert(
                "chunk_index".to_string(),
                serde_json::Value::Number(chunk_index.into()),
            );
        }

        // Add extra metadata
        for (key, value) in &metadata.extra {
            result.insert(key.clone(), value.clone());
        }

        result
    }

    /// Convert Pinecone metadata to VectorMetadata
    fn pinecone_to_metadata(metadata: &HashMap<String, serde_json::Value>) -> VectorMetadata {
        let mut result = VectorMetadata::new();

        if let Some(serde_json::Value::String(text)) = metadata.get("text") {
            result.text = Some(text.clone());
        }

        if let Some(serde_json::Value::String(source)) = metadata.get("source") {
            result.source = Some(source.clone());
        }

        if let Some(serde_json::Value::Number(n)) = metadata.get("chunk_index") {
            if let Some(idx) = n.as_u64() {
                result.chunk_index = Some(idx as u32);
            }
        }

        // Copy remaining fields to extra
        for (key, value) in metadata {
            if key != "text" && key != "source" && key != "chunk_index" {
                result.extra.insert(key.clone(), value.clone());
            }
        }

        result
    }

    /// Convert VectorFilter to Pinecone filter format
    fn filter_to_pinecone(filter: &VectorFilter) -> HashMap<String, serde_json::Value> {
        let mut result = HashMap::new();

        // Pinecone uses a different filter syntax:
        // { "field": { "$eq": value } }
        // For simplicity, we only handle must conditions with equals operations

        if !filter.must.is_empty() {
            let mut and_conditions = Vec::new();

            for condition in &filter.must {
                let mut field_filter = HashMap::new();
                field_filter.insert("$eq".to_string(), condition.value.clone());

                let mut condition_obj = HashMap::new();
                condition_obj.insert(
                    condition.field.clone(),
                    serde_json::to_value(field_filter).unwrap_or_default(),
                );

                and_conditions.push(serde_json::to_value(condition_obj).unwrap_or_default());
            }

            if and_conditions.len() == 1 {
                // Single condition doesn't need $and wrapper
                if let serde_json::Value::Object(obj) = &and_conditions[0] {
                    for (k, v) in obj {
                        result.insert(k.clone(), v.clone());
                    }
                }
            } else {
                result.insert("$and".to_string(), serde_json::Value::Array(and_conditions));
            }
        }

        result
    }

    /// Make an authenticated request to Pinecone
    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<impl Serialize>,
    ) -> Result<T> {
        let url = self.url(path);

        let mut request = self
            .client
            .request(method.clone(), &url)
            .header("Api-Key", &self.api_key)
            .header("Content-Type", "application/json");

        if let Some(body) = body {
            request = request.json(&body);
        }

        let response = request
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Pinecone request failed: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            let error_msg = if let Ok(err) = serde_json::from_str::<PineconeError>(&error_text) {
                err.message
            } else {
                error_text
            };

            return Err(SdkError::Other(anyhow::anyhow!(
                "Pinecone API error ({}): {}",
                status,
                error_msg
            )));
        }

        response.json().await.map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to parse Pinecone response: {}", e))
        })
    }
}

#[async_trait]
impl VectorDatabase for PineconeProvider {
    fn provider_name(&self) -> &'static str {
        "pinecone"
    }

    async fn health_check(&self) -> Result<()> {
        // Use describe_index_stats as health check
        let _stats: DescribeIndexStatsResponse = self
            .request(reqwest::Method::GET, "/describe_index_stats", None::<()>)
            .await?;
        Ok(())
    }

    async fn create_collection(&self, _collection: &Collection) -> Result<()> {
        // Pinecone indexes are created via the control plane API or console,
        // not the data plane API we use here. Collections map to namespaces.
        tracing::info!(
            "Pinecone: create_collection is a no-op. Indexes are created via Pinecone console or control plane API. \
             Use namespaces for logical separation within an index."
        );
        Ok(())
    }

    async fn delete_collection(&self, name: &str) -> Result<()> {
        // Delete all vectors in a namespace
        let request = DeleteRequest {
            ids: None,
            delete_all: Some(true),
            namespace: Some(name.to_string()),
            filter: None,
        };

        // Pinecone delete returns an empty object on success
        let _: serde_json::Value = self
            .request(reqwest::Method::POST, "/vectors/delete", Some(request))
            .await?;

        Ok(())
    }

    async fn list_collections(&self) -> Result<Vec<String>> {
        // Return namespaces from index stats
        let stats: DescribeIndexStatsResponse = self
            .request(reqwest::Method::GET, "/describe_index_stats", None::<()>)
            .await?;

        let namespaces = stats
            .namespaces
            .map(|ns| ns.keys().cloned().collect())
            .unwrap_or_else(Vec::new);

        Ok(namespaces)
    }

    async fn upsert_vectors(&self, collection_name: &str, vectors: Vec<VectorEntry>) -> Result<()> {
        // Pinecone has a limit of 100 vectors per upsert request
        const BATCH_SIZE: usize = 100;

        for chunk in vectors.chunks(BATCH_SIZE) {
            let pinecone_vectors: Vec<PineconeVector> = chunk
                .iter()
                .map(|entry| {
                    let metadata = if entry.metadata.text.is_some()
                        || entry.metadata.source.is_some()
                        || entry.metadata.chunk_index.is_some()
                        || !entry.metadata.extra.is_empty()
                    {
                        Some(Self::metadata_to_pinecone(&entry.metadata))
                    } else {
                        None
                    };

                    PineconeVector {
                        id: entry.id.clone(),
                        values: entry.vector.clone(),
                        metadata,
                    }
                })
                .collect();

            let request = UpsertRequest {
                vectors: pinecone_vectors,
                namespace: Some(collection_name.to_string()),
            };

            // Pinecone upsert returns { "upsertedCount": n }
            let _: serde_json::Value = self
                .request(reqwest::Method::POST, "/vectors/upsert", Some(request))
                .await?;
        }

        Ok(())
    }

    async fn search_vectors(
        &self,
        collection_name: &str,
        query: SearchQuery,
    ) -> Result<Vec<SearchResult>> {
        let filter = query.filter.as_ref().map(Self::filter_to_pinecone);

        let request = QueryRequest {
            vector: query.vector,
            top_k: query.limit,
            include_metadata: query.include_metadata,
            include_values: query.include_vectors,
            namespace: Some(collection_name.to_string()),
            filter,
        };

        let response: QueryResponse = self
            .request(reqwest::Method::POST, "/query", Some(request))
            .await?;

        let results = response
            .matches
            .into_iter()
            .filter(|m| {
                // Apply min_score filter if specified
                query.min_score.map_or(true, |min| m.score >= min)
            })
            .map(|m| {
                let metadata = m.metadata.as_ref().map(Self::pinecone_to_metadata);

                SearchResult {
                    id: m.id,
                    score: m.score,
                    distance: 1.0 - m.score, // Convert similarity to distance
                    vector: m.values,
                    metadata,
                }
            })
            .collect();

        Ok(results)
    }

    async fn delete_vectors(&self, collection_name: &str, ids: Vec<String>) -> Result<()> {
        let request = DeleteRequest {
            ids: Some(ids),
            delete_all: None,
            namespace: Some(collection_name.to_string()),
            filter: None,
        };

        let _: serde_json::Value = self
            .request(reqwest::Method::POST, "/vectors/delete", Some(request))
            .await?;

        Ok(())
    }

    async fn delete_by_filter(&self, collection_name: &str, filter: VectorFilter) -> Result<()> {
        let pinecone_filter = Self::filter_to_pinecone(&filter);

        let request = DeleteRequest {
            ids: None,
            delete_all: None,
            namespace: Some(collection_name.to_string()),
            filter: Some(pinecone_filter),
        };

        let _: serde_json::Value = self
            .request(reqwest::Method::POST, "/vectors/delete", Some(request))
            .await?;

        Ok(())
    }

    async fn get_vector(&self, collection_name: &str, id: &str) -> Result<Option<VectorEntry>> {
        // Build fetch URL with query parameters
        let url = format!(
            "/vectors/fetch?ids={}&namespace={}",
            urlencoding::encode(id),
            urlencoding::encode(collection_name)
        );

        let response: FetchResponse = self.request(reqwest::Method::GET, &url, None::<()>).await?;

        Ok(response.vectors.get(id).map(|v| VectorEntry {
            id: v.id.clone(),
            vector: v.values.clone(),
            metadata: v
                .metadata
                .as_ref()
                .map(Self::pinecone_to_metadata)
                .unwrap_or_default(),
        }))
    }

    async fn collection_info(&self, collection_name: &str) -> Result<CollectionInfo> {
        let stats: DescribeIndexStatsResponse = self
            .request(reqwest::Method::GET, "/describe_index_stats", None::<()>)
            .await?;

        let vector_count = stats
            .namespaces
            .as_ref()
            .and_then(|ns| ns.get(collection_name))
            .map(|s| s.vector_count)
            .unwrap_or(0);

        Ok(CollectionInfo {
            name: collection_name.to_string(),
            vector_count,
            indexed_vector_count: vector_count,
            points_count: vector_count,
            segments_count: 1, // Pinecone doesn't expose this
            status: "ready".to_string(),
            dimension: stats.dimension,
            distance_metric: DistanceMetric::Cosine, // Pinecone defaults to cosine
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_conversion() {
        let metadata = VectorMetadata {
            text: Some("test content".to_string()),
            source: Some("test.txt".to_string()),
            chunk_index: Some(5),
            extra: {
                let mut map = HashMap::new();
                map.insert(
                    "custom".to_string(),
                    serde_json::Value::String("value".to_string()),
                );
                map
            },
        };

        let pinecone = PineconeProvider::metadata_to_pinecone(&metadata);
        assert_eq!(
            pinecone.get("text"),
            Some(&serde_json::Value::String("test content".to_string()))
        );
        assert_eq!(
            pinecone.get("source"),
            Some(&serde_json::Value::String("test.txt".to_string()))
        );
        assert_eq!(
            pinecone.get("chunk_index"),
            Some(&serde_json::Value::Number(5.into()))
        );
        assert_eq!(
            pinecone.get("custom"),
            Some(&serde_json::Value::String("value".to_string()))
        );

        // Round-trip
        let back = PineconeProvider::pinecone_to_metadata(&pinecone);
        assert_eq!(back.text, metadata.text);
        assert_eq!(back.source, metadata.source);
        assert_eq!(back.chunk_index, metadata.chunk_index);
    }

    #[test]
    fn test_filter_conversion() {
        use super::super::super::types::FilterCondition;

        let filter = VectorFilter::new().must(FilterCondition::equals(
            "source".to_string(),
            serde_json::Value::String("doc.pdf".to_string()),
        ));

        let pinecone_filter = PineconeProvider::filter_to_pinecone(&filter);
        assert!(pinecone_filter.contains_key("source"));
    }
}
