// Embedder trait and implementations for generating text embeddings
// Used by SemanticMemory for vectorizing text content

use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::{Result as SdkResult, SdkError};

// ============================================================================
// Embedder Trait
// ============================================================================

/// Core trait for embedding providers.
/// Implementations convert text into dense vector representations.
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Generate embedding for a single text
    async fn embed(&self, text: &str) -> SdkResult<Vec<f32>>;

    /// Generate embeddings for multiple texts (batch operation)
    /// Default implementation calls embed() sequentially; providers can override for efficiency
    async fn embed_batch(&self, texts: &[&str]) -> SdkResult<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    /// Get the vector dimension for this embedder
    fn dimension(&self) -> u32;

    /// Get the provider name (for logging/debugging)
    fn provider_name(&self) -> &'static str;

    /// Get the model name
    fn model_name(&self) -> &str;
}

// ============================================================================
// OpenAI Embedder
// ============================================================================

const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_EMBEDDINGS_PATH: &str = "embeddings";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// OpenAI embedding models and their dimensions
#[derive(Clone, Debug, PartialEq)]
pub enum OpenAiEmbeddingModel {
    /// text-embedding-3-small: 1536 dimensions, cheaper, faster
    TextEmbedding3Small,
    /// text-embedding-3-large: 3072 dimensions, more accurate
    TextEmbedding3Large,
    /// text-embedding-ada-002: 1536 dimensions (legacy)
    TextEmbeddingAda002,
}

impl OpenAiEmbeddingModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            OpenAiEmbeddingModel::TextEmbedding3Small => "text-embedding-3-small",
            OpenAiEmbeddingModel::TextEmbedding3Large => "text-embedding-3-large",
            OpenAiEmbeddingModel::TextEmbeddingAda002 => "text-embedding-ada-002",
        }
    }

    pub fn dimension(&self) -> u32 {
        match self {
            OpenAiEmbeddingModel::TextEmbedding3Small => 1536,
            OpenAiEmbeddingModel::TextEmbedding3Large => 3072,
            OpenAiEmbeddingModel::TextEmbeddingAda002 => 1536,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "text-embedding-3-small" => Some(OpenAiEmbeddingModel::TextEmbedding3Small),
            "text-embedding-3-large" => Some(OpenAiEmbeddingModel::TextEmbedding3Large),
            "text-embedding-ada-002" => Some(OpenAiEmbeddingModel::TextEmbeddingAda002),
            _ => None,
        }
    }
}

impl Default for OpenAiEmbeddingModel {
    fn default() -> Self {
        OpenAiEmbeddingModel::TextEmbedding3Small
    }
}

/// Configuration for OpenAI Embeddings API
#[derive(Clone, Debug)]
pub struct OpenAiEmbedderConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: OpenAiEmbeddingModel,
    pub timeout: Duration,
    pub organization: Option<String>,
    pub project: Option<String>,
}

impl OpenAiEmbedderConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_OPENAI_BASE_URL.to_string(),
            model: OpenAiEmbeddingModel::default(),
            timeout: DEFAULT_TIMEOUT,
            organization: None,
            project: None,
        }
    }

    pub fn with_model(mut self, model: OpenAiEmbeddingModel) -> Self {
        self.model = model;
        self
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_organization(mut self, organization: impl Into<String>) -> Self {
        self.organization = Some(organization.into());
        self
    }

    pub fn with_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    /// Create config from environment variables
    /// OPENAI_API_KEY (required)
    /// OPENAI_BASE_URL (optional)
    /// OPENAI_EMBEDDING_MODEL (optional, defaults to text-embedding-3-small)
    /// OPENAI_ORGANIZATION (optional)
    /// OPENAI_PROJECT (optional)
    pub fn from_env() -> SdkResult<Self> {
        let api_key = env::var("OPENAI_API_KEY").map_err(|_| SdkError::Configuration {
            message: "OPENAI_API_KEY must be set".to_string(),
            field: Some("OPENAI_API_KEY".to_string()),
        })?;

        let mut config = Self::new(api_key);

        if let Ok(base_url) = env::var("OPENAI_BASE_URL") {
            if !base_url.trim().is_empty() {
                config.base_url = base_url;
            }
        }

        if let Ok(model_str) = env::var("OPENAI_EMBEDDING_MODEL") {
            if let Some(model) = OpenAiEmbeddingModel::from_str(&model_str) {
                config.model = model;
            } else {
                tracing::warn!(
                    "Unknown OPENAI_EMBEDDING_MODEL '{}', using default",
                    model_str
                );
            }
        }

        if let Ok(org) = env::var("OPENAI_ORGANIZATION") {
            if !org.trim().is_empty() {
                config.organization = Some(org);
            }
        }

        if let Ok(project) = env::var("OPENAI_PROJECT") {
            if !project.trim().is_empty() {
                config.project = Some(project);
            }
        }

        Ok(config)
    }
}

/// OpenAI Embeddings API provider
pub struct OpenAiEmbedder {
    config: OpenAiEmbedderConfig,
    client: Client,
}

impl OpenAiEmbedder {
    pub fn new(config: OpenAiEmbedderConfig) -> SdkResult<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| SdkError::Other(anyhow!("Failed to create HTTP client: {}", e)))?;

        Ok(Self { config, client })
    }

    /// Create embedder from environment variables
    pub fn from_env() -> SdkResult<Self> {
        let config = OpenAiEmbedderConfig::from_env()?;
        Self::new(config)
    }

    fn build_request_headers(&self) -> SdkResult<reqwest::header::HeaderMap> {
        let mut headers = reqwest::header::HeaderMap::new();

        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );

        let auth_value = format!("Bearer {}", self.config.api_key);
        headers.insert(
            reqwest::header::AUTHORIZATION,
            auth_value.parse().map_err(|_| SdkError::Configuration {
                message: "Invalid API key format".to_string(),
                field: Some("api_key".to_string()),
            })?,
        );

        if let Some(ref org) = self.config.organization {
            headers.insert(
                "OpenAI-Organization",
                org.parse().map_err(|_| SdkError::Configuration {
                    message: "Invalid organization format".to_string(),
                    field: Some("organization".to_string()),
                })?,
            );
        }

        if let Some(ref project) = self.config.project {
            headers.insert(
                "OpenAI-Project",
                project.parse().map_err(|_| SdkError::Configuration {
                    message: "Invalid project format".to_string(),
                    field: Some("project".to_string()),
                })?,
            );
        }

        Ok(headers)
    }
}

// OpenAI API request/response types
#[derive(Debug, Serialize)]
struct EmbeddingRequest {
    input: Vec<String>,
    model: String,
    encoding_format: String,
}

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
    model: String,
    usage: EmbeddingUsage,
}

#[derive(Debug, Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct EmbeddingUsage {
    prompt_tokens: u32,
    total_tokens: u32,
}

#[derive(Debug, Deserialize)]
struct OpenAiErrorResponse {
    error: OpenAiError,
}

#[derive(Debug, Deserialize)]
struct OpenAiError {
    message: String,
    r#type: Option<String>,
    code: Option<String>,
}

#[async_trait]
impl Embedder for OpenAiEmbedder {
    async fn embed(&self, text: &str) -> SdkResult<Vec<f32>> {
        let results = self.embed_batch(&[text]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| SdkError::Other(anyhow!("Empty embedding response")))
    }

    async fn embed_batch(&self, texts: &[&str]) -> SdkResult<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        let url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            DEFAULT_EMBEDDINGS_PATH
        );

        let request_body = EmbeddingRequest {
            input: texts.iter().map(|s| s.to_string()).collect(),
            model: self.config.model.as_str().to_string(),
            encoding_format: "float".to_string(),
        };

        let headers = self.build_request_headers()?;

        tracing::debug!(
            "Sending embedding request for {} texts to {}",
            texts.len(),
            url
        );

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow!("Embedding request failed: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            let error_msg = if let Ok(error_response) =
                serde_json::from_str::<OpenAiErrorResponse>(&error_text)
            {
                format!(
                    "OpenAI API error: {} (type: {:?}, code: {:?})",
                    error_response.error.message,
                    error_response.error.r#type,
                    error_response.error.code
                )
            } else {
                format!("OpenAI API error ({}): {}", status, error_text)
            };

            return Err(SdkError::Other(anyhow!(error_msg)));
        }

        let embedding_response: EmbeddingResponse = response
            .json()
            .await
            .map_err(|e| SdkError::Other(anyhow!("Failed to parse embedding response: {}", e)))?;

        tracing::debug!(
            "Received {} embeddings (model: {}, tokens: {})",
            embedding_response.data.len(),
            embedding_response.model,
            embedding_response.usage.total_tokens
        );

        // Sort by index to ensure correct order
        let mut embeddings: Vec<(usize, Vec<f32>)> = embedding_response
            .data
            .into_iter()
            .map(|d| (d.index, d.embedding))
            .collect();
        embeddings.sort_by_key(|(idx, _)| *idx);

        Ok(embeddings.into_iter().map(|(_, e)| e).collect())
    }

    fn dimension(&self) -> u32 {
        self.config.model.dimension()
    }

    fn provider_name(&self) -> &'static str {
        "openai"
    }

    fn model_name(&self) -> &str {
        self.config.model.as_str()
    }
}

// ============================================================================
// Embedder Registry
// ============================================================================

/// Registry for managing multiple embedder providers
pub struct EmbedderRegistry {
    providers: HashMap<String, Arc<dyn Embedder>>,
    default_provider: Option<String>,
}

impl EmbedderRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
            default_provider: None,
        }
    }

    /// Register an embedder provider
    pub fn register_provider(&mut self, name: String, provider: Arc<dyn Embedder>) {
        tracing::info!(
            "Registering embedder provider: {} (type: {}, model: {})",
            name,
            provider.provider_name(),
            provider.model_name()
        );
        self.providers.insert(name, provider);
    }

    /// Get a provider by name
    pub fn get_provider(&self, name: &str) -> Option<Arc<dyn Embedder>> {
        self.providers.get(name).cloned()
    }

    /// Set the default provider
    pub fn set_default_provider(&mut self, name: String) -> SdkResult<()> {
        if self.providers.contains_key(&name) {
            self.default_provider = Some(name);
            Ok(())
        } else {
            Err(SdkError::Other(anyhow!(
                "Embedder provider not found: {}",
                name
            )))
        }
    }

    /// Get the default provider
    pub fn get_default_provider(&self) -> Option<Arc<dyn Embedder>> {
        self.default_provider
            .as_ref()
            .and_then(|name| self.get_provider(name))
    }

    /// List all registered provider names
    pub fn list_providers(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    /// Load providers from environment variables
    /// Currently supports:
    /// - OpenAI (OPENAI_API_KEY)
    pub fn load_from_environment(&mut self) -> SdkResult<()> {
        let mut loaded_count = 0;

        // OpenAI embeddings
        if env::var("OPENAI_API_KEY").is_ok() {
            match OpenAiEmbedder::from_env() {
                Ok(provider) => {
                    self.register_provider("openai".to_string(), Arc::new(provider));
                    loaded_count += 1;

                    if self.default_provider.is_none() {
                        self.default_provider = Some("openai".to_string());
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to load OpenAI embedder: {}", e);
                }
            }
        }

        if loaded_count == 0 {
            tracing::warn!(
                "No embedder providers loaded from environment. Set OPENAI_API_KEY for embeddings."
            );
            return Err(SdkError::Other(anyhow!(
                "No embedder providers available"
            )));
        }

        tracing::info!(
            "Loaded {} embedder providers from environment",
            loaded_count
        );

        Ok(())
    }
}

impl Default for EmbedderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_model_dimensions() {
        assert_eq!(OpenAiEmbeddingModel::TextEmbedding3Small.dimension(), 1536);
        assert_eq!(OpenAiEmbeddingModel::TextEmbedding3Large.dimension(), 3072);
        assert_eq!(OpenAiEmbeddingModel::TextEmbeddingAda002.dimension(), 1536);
    }

    #[test]
    fn test_embedding_model_from_str() {
        assert_eq!(
            OpenAiEmbeddingModel::from_str("text-embedding-3-small"),
            Some(OpenAiEmbeddingModel::TextEmbedding3Small)
        );
        assert_eq!(
            OpenAiEmbeddingModel::from_str("text-embedding-3-large"),
            Some(OpenAiEmbeddingModel::TextEmbedding3Large)
        );
        assert_eq!(OpenAiEmbeddingModel::from_str("unknown-model"), None);
    }

    #[test]
    fn test_config_builder() {
        let config = OpenAiEmbedderConfig::new("test-key")
            .with_model(OpenAiEmbeddingModel::TextEmbedding3Large)
            .with_organization("test-org");

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.model, OpenAiEmbeddingModel::TextEmbedding3Large);
        assert_eq!(config.organization, Some("test-org".to_string()));
    }

    #[test]
    fn test_registry() {
        let registry = EmbedderRegistry::new();
        assert!(registry.list_providers().is_empty());
        assert!(registry.get_default_provider().is_none());
    }
}
