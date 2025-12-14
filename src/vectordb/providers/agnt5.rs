// AGNT5 Platform vector database provider
// Calls the platform's API for vector operations
// Platform handles storage: SQLite (Embedded), PostgreSQL (Community), CockroachDB+Qdrant (Managed)

use std::env;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::super::{
    Collection, CollectionInfo, DistanceMetric, SearchQuery, SearchResult, VectorDatabase,
    VectorEntry, VectorFilter, VectorMetadata,
};
use crate::error::{Result, SdkError};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Configuration for AGNT5 platform vector provider
#[derive(Clone, Debug)]
pub struct Agnt5ProviderConfig {
    /// Platform gateway URL (e.g., http://localhost:34183)
    pub gateway_url: String,
    /// API key for authentication (optional for embedded mode)
    pub api_key: Option<String>,
    /// Request timeout
    pub timeout: Duration,
    /// Tenant ID (for multi-tenant deployments)
    pub tenant_id: Option<String>,
    /// Deployment ID
    pub deployment_id: Option<String>,
}

impl Agnt5ProviderConfig {
    pub fn new(gateway_url: impl Into<String>) -> Self {
        Self {
            gateway_url: gateway_url.into(),
            api_key: None,
            timeout: DEFAULT_TIMEOUT,
            tenant_id: None,
            deployment_id: None,
        }
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    pub fn with_deployment_id(mut self, deployment_id: impl Into<String>) -> Self {
        self.deployment_id = Some(deployment_id.into());
        self
    }

    /// Create config from environment variables
    /// AGNT5_GATEWAY_URL or AGNT5_PLATFORM_URL (required)
    /// AGNT5_API_KEY (optional)
    /// AGNT5_TENANT_ID (optional)
    /// AGNT5_DEPLOYMENT_ID (optional)
    pub fn from_env() -> Result<Self> {
        let gateway_url = env::var("AGNT5_GATEWAY_URL")
            .or_else(|_| env::var("AGNT5_PLATFORM_URL"))
            .unwrap_or_else(|_| "http://localhost:34183".to_string());

        let mut config = Self::new(gateway_url);

        if let Ok(api_key) = env::var("AGNT5_API_KEY") {
            if !api_key.trim().is_empty() {
                config.api_key = Some(api_key);
            }
        }

        if let Ok(tenant_id) = env::var("AGNT5_TENANT_ID") {
            if !tenant_id.trim().is_empty() {
                config.tenant_id = Some(tenant_id);
            }
        }

        if let Ok(deployment_id) = env::var("AGNT5_DEPLOYMENT_ID") {
            if !deployment_id.trim().is_empty() {
                config.deployment_id = Some(deployment_id);
            }
        }

        Ok(config)
    }
}

/// AGNT5 Platform vector database provider
/// Routes vector operations to the platform API
pub struct Agnt5Provider {
    config: Agnt5ProviderConfig,
    client: Client,
}

impl Agnt5Provider {
    pub fn new(config: Agnt5ProviderConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to create HTTP client: {}", e)))?;

        Ok(Self { config, client })
    }

    /// Create provider from environment variables
    pub fn from_env() -> Result<Self> {
        let config = Agnt5ProviderConfig::from_env()?;
        Self::new(config)
    }

    fn build_url(&self, path: &str) -> String {
        format!(
            "{}/v1/vectors{}",
            self.config.gateway_url.trim_end_matches('/'),
            path
        )
    }

    fn build_headers(&self) -> Result<reqwest::header::HeaderMap> {
        let mut headers = reqwest::header::HeaderMap::new();

        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );

        if let Some(ref api_key) = self.config.api_key {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", api_key)
                    .parse()
                    .map_err(|_| SdkError::Configuration {
                        message: "Invalid API key format".to_string(),
                        field: Some("api_key".to_string()),
                    })?,
            );
        }

        if let Some(ref tenant_id) = self.config.tenant_id {
            headers.insert(
                "X-Tenant-ID",
                tenant_id.parse().map_err(|_| SdkError::Configuration {
                    message: "Invalid tenant ID format".to_string(),
                    field: Some("tenant_id".to_string()),
                })?,
            );
        }

        if let Some(ref deployment_id) = self.config.deployment_id {
            headers.insert(
                "X-Deployment-ID",
                deployment_id
                    .parse()
                    .map_err(|_| SdkError::Configuration {
                        message: "Invalid deployment ID format".to_string(),
                        field: Some("deployment_id".to_string()),
                    })?,
            );
        }

        Ok(headers)
    }

    async fn handle_response<T: for<'de> Deserialize<'de>>(
        &self,
        response: reqwest::Response,
    ) -> Result<T> {
        let status = response.status();

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            let error_msg = if let Ok(error_response) =
                serde_json::from_str::<ApiErrorResponse>(&error_text)
            {
                error_response.error
            } else {
                format!("API error ({}): {}", status, error_text)
            };

            return Err(SdkError::Other(anyhow::anyhow!(error_msg)));
        }

        response
            .json()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to parse response: {}", e)))
    }
}

// ============================================================================
// API Request/Response Types
// ============================================================================

#[derive(Debug, Deserialize)]
struct ApiErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
struct CreateCollectionRequest {
    name: String,
    dimension: u32,
    distance_metric: String,
    description: Option<String>,
}

#[derive(Debug, Serialize)]
struct UpsertVectorsRequest {
    vectors: Vec<VectorEntryDto>,
}

#[derive(Debug, Serialize, Deserialize)]
struct VectorEntryDto {
    id: String,
    vector: Vec<f32>,
    metadata: VectorMetadataDto,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct VectorMetadataDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunk_index: Option<u32>,
    #[serde(default)]
    extra: std::collections::HashMap<String, serde_json::Value>,
}

impl From<&VectorMetadata> for VectorMetadataDto {
    fn from(m: &VectorMetadata) -> Self {
        Self {
            text: m.text.clone(),
            source: m.source.clone(),
            chunk_index: m.chunk_index,
            extra: m.extra.clone(),
        }
    }
}

impl From<VectorMetadataDto> for VectorMetadata {
    fn from(m: VectorMetadataDto) -> Self {
        VectorMetadata {
            text: m.text,
            source: m.source,
            chunk_index: m.chunk_index,
            extra: m.extra,
        }
    }
}

impl From<&VectorEntry> for VectorEntryDto {
    fn from(e: &VectorEntry) -> Self {
        Self {
            id: e.id.clone(),
            vector: e.vector.clone(),
            metadata: VectorMetadataDto::from(&e.metadata),
        }
    }
}

impl From<VectorEntryDto> for VectorEntry {
    fn from(e: VectorEntryDto) -> Self {
        VectorEntry {
            id: e.id,
            vector: e.vector,
            metadata: VectorMetadata::from(e.metadata),
        }
    }
}

#[derive(Debug, Serialize)]
struct SearchVectorsRequest {
    vector: Vec<f32>,
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    distance_metric: Option<String>,
    include_vectors: bool,
    include_metadata: bool,
}

#[derive(Debug, Deserialize)]
struct SearchVectorsResponse {
    results: Vec<SearchResultDto>,
}

#[derive(Debug, Deserialize)]
struct SearchResultDto {
    id: String,
    score: f32,
    distance: f32,
    #[serde(default)]
    vector: Option<Vec<f32>>,
    #[serde(default)]
    metadata: Option<VectorMetadataDto>,
}

impl From<SearchResultDto> for SearchResult {
    fn from(r: SearchResultDto) -> Self {
        SearchResult {
            id: r.id,
            score: r.score,
            distance: r.distance,
            vector: r.vector,
            metadata: r.metadata.map(VectorMetadata::from),
        }
    }
}

#[derive(Debug, Serialize)]
struct DeleteVectorsRequest {
    ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ListCollectionsResponse {
    collections: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CollectionInfoResponse {
    name: String,
    vector_count: u64,
    indexed_vector_count: u64,
    points_count: u64,
    segments_count: u32,
    status: String,
    dimension: u32,
    distance_metric: String,
}

#[derive(Debug, Deserialize)]
struct HealthResponse {
    status: String,
}

#[derive(Debug, Deserialize)]
struct GetVectorResponse {
    #[serde(flatten)]
    vector: Option<VectorEntryDto>,
}

// ============================================================================
// VectorDatabase Implementation
// ============================================================================

#[async_trait]
impl VectorDatabase for Agnt5Provider {
    fn provider_name(&self) -> &'static str {
        "agnt5"
    }

    async fn health_check(&self) -> Result<()> {
        let url = self.build_url("/health");
        let headers = self.build_headers()?;

        let response = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Health check request failed: {}", e)))?;

        let health: HealthResponse = self.handle_response(response).await?;

        if health.status == "ok" || health.status == "healthy" {
            Ok(())
        } else {
            Err(SdkError::Other(anyhow::anyhow!(
                "Platform unhealthy: {}",
                health.status
            )))
        }
    }

    async fn create_collection(&self, collection: &Collection) -> Result<()> {
        let url = self.build_url("/collections");
        let headers = self.build_headers()?;

        let request = CreateCollectionRequest {
            name: collection.name.clone(),
            dimension: collection.dimension,
            distance_metric: collection.distance_metric.to_string(),
            description: collection.description.clone(),
        };

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Create collection request failed: {}", e))
            })?;

        let _: serde_json::Value = self.handle_response(response).await?;

        tracing::info!(
            "Created collection '{}' via AGNT5 platform",
            collection.name
        );

        Ok(())
    }

    async fn delete_collection(&self, name: &str) -> Result<()> {
        let url = self.build_url(&format!("/collections/{}", name));
        let headers = self.build_headers()?;

        let response = self
            .client
            .delete(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Delete collection request failed: {}", e))
            })?;

        let _: serde_json::Value = self.handle_response(response).await?;

        tracing::info!("Deleted collection '{}' via AGNT5 platform", name);

        Ok(())
    }

    async fn list_collections(&self) -> Result<Vec<String>> {
        let url = self.build_url("/collections");
        let headers = self.build_headers()?;

        let response = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("List collections request failed: {}", e))
            })?;

        let result: ListCollectionsResponse = self.handle_response(response).await?;

        Ok(result.collections)
    }

    async fn upsert_vectors(&self, collection_name: &str, vectors: Vec<VectorEntry>) -> Result<()> {
        let url = self.build_url(&format!("/collections/{}/vectors", collection_name));
        let headers = self.build_headers()?;

        let request = UpsertVectorsRequest {
            vectors: vectors.iter().map(VectorEntryDto::from).collect(),
        };

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Upsert vectors request failed: {}", e))
            })?;

        let _: serde_json::Value = self.handle_response(response).await?;

        tracing::debug!(
            "Upserted {} vectors to '{}' via AGNT5 platform",
            vectors.len(),
            collection_name
        );

        Ok(())
    }

    async fn search_vectors(
        &self,
        collection_name: &str,
        query: SearchQuery,
    ) -> Result<Vec<SearchResult>> {
        let url = self.build_url(&format!("/collections/{}/search", collection_name));
        let headers = self.build_headers()?;

        let request = SearchVectorsRequest {
            vector: query.vector,
            limit: query.limit,
            min_score: query.min_score,
            distance_metric: query.distance_metric.map(|m| m.to_string()),
            include_vectors: query.include_vectors,
            include_metadata: query.include_metadata,
        };

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Search vectors request failed: {}", e))
            })?;

        let result: SearchVectorsResponse = self.handle_response(response).await?;

        Ok(result.results.into_iter().map(SearchResult::from).collect())
    }

    async fn delete_vectors(&self, collection_name: &str, ids: Vec<String>) -> Result<()> {
        let url = self.build_url(&format!("/collections/{}/vectors", collection_name));
        let headers = self.build_headers()?;

        let request = DeleteVectorsRequest { ids };

        let response = self
            .client
            .delete(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Delete vectors request failed: {}", e))
            })?;

        let _: serde_json::Value = self.handle_response(response).await?;

        Ok(())
    }

    async fn delete_by_filter(&self, collection_name: &str, filter: VectorFilter) -> Result<()> {
        let url = self.build_url(&format!("/collections/{}/vectors/filter", collection_name));
        let headers = self.build_headers()?;

        let response = self
            .client
            .delete(&url)
            .headers(headers)
            .json(&filter)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Delete by filter request failed: {}", e))
            })?;

        let _: serde_json::Value = self.handle_response(response).await?;

        Ok(())
    }

    async fn get_vector(&self, collection_name: &str, id: &str) -> Result<Option<VectorEntry>> {
        let url = self.build_url(&format!("/collections/{}/vectors/{}", collection_name, id));
        let headers = self.build_headers()?;

        let response = self.client.get(&url).headers(headers).send().await;

        match response {
            Ok(resp) => {
                if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }

                let result: GetVectorResponse = self.handle_response(resp).await?;
                Ok(result.vector.map(VectorEntry::from))
            }
            Err(e) => Err(SdkError::Other(anyhow::anyhow!(
                "Get vector request failed: {}",
                e
            ))),
        }
    }

    async fn collection_info(&self, collection_name: &str) -> Result<CollectionInfo> {
        let url = self.build_url(&format!("/collections/{}", collection_name));
        let headers = self.build_headers()?;

        let response = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Collection info request failed: {}", e))
            })?;

        let info: CollectionInfoResponse = self.handle_response(response).await?;

        let distance_metric = match info.distance_metric.as_str() {
            "euclidean" => DistanceMetric::Euclidean,
            "dot_product" => DistanceMetric::DotProduct,
            "manhattan" => DistanceMetric::Manhattan,
            _ => DistanceMetric::Cosine,
        };

        Ok(CollectionInfo {
            name: info.name,
            vector_count: info.vector_count,
            indexed_vector_count: info.indexed_vector_count,
            points_count: info.points_count,
            segments_count: info.segments_count,
            status: info.status,
            dimension: info.dimension,
            distance_metric,
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = Agnt5ProviderConfig::new("http://localhost:34183")
            .with_api_key("test-key")
            .with_tenant_id("tenant-123");

        assert_eq!(config.gateway_url, "http://localhost:34183");
        assert_eq!(config.api_key, Some("test-key".to_string()));
        assert_eq!(config.tenant_id, Some("tenant-123".to_string()));
    }

    #[test]
    fn test_url_building() {
        let config = Agnt5ProviderConfig::new("http://localhost:34183");
        let provider = Agnt5Provider::new(config).unwrap();

        assert_eq!(
            provider.build_url("/collections"),
            "http://localhost:34183/v1/vectors/collections"
        );
        assert_eq!(
            provider.build_url("/collections/test/vectors"),
            "http://localhost:34183/v1/vectors/collections/test/vectors"
        );
    }

    #[test]
    fn test_metadata_conversion() {
        let metadata = VectorMetadata {
            text: Some("test text".to_string()),
            source: Some("test source".to_string()),
            chunk_index: Some(5),
            extra: std::collections::HashMap::new(),
        };

        let dto = VectorMetadataDto::from(&metadata);
        assert_eq!(dto.text, Some("test text".to_string()));
        assert_eq!(dto.source, Some("test source".to_string()));
        assert_eq!(dto.chunk_index, Some(5));

        let recovered = VectorMetadata::from(dto);
        assert_eq!(recovered.text, metadata.text);
        assert_eq!(recovered.source, metadata.source);
        assert_eq!(recovered.chunk_index, metadata.chunk_index);
    }
}
