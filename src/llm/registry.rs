// Provider registry for dynamic LLM provider management
use std::collections::HashMap;
use std::sync::Arc;

use super::provider::{Provider, ProviderConfig, ProviderType};
use crate::error::{Result, SdkError};

pub struct LlmRegistry {
    providers: HashMap<String, Arc<dyn Provider>>,
    default_provider: Option<String>,
}

impl LlmRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
            default_provider: None,
        }
    }

    /// Register a provider with the given name
    pub fn register_provider(&mut self, name: String, provider: Arc<dyn Provider>) {
        tracing::info!(
            "Registering LLM provider: {} (type: {})",
            name,
            provider.r#type()
        );
        self.providers.insert(name, provider);
    }

    /// Get a provider by name
    pub fn get_provider(&self, name: &str) -> Option<Arc<dyn Provider>> {
        self.providers.get(name).cloned()
    }

    /// Set the default provider
    pub fn set_default_provider(&mut self, name: String) -> Result<()> {
        if self.providers.contains_key(&name) {
            self.default_provider = Some(name);
            Ok(())
        } else {
            Err(SdkError::Other(anyhow::anyhow!(
                "Provider not found: {}",
                name
            )))
        }
    }

    /// Get the default provider
    pub fn get_default_provider(&self) -> Option<Arc<dyn Provider>> {
        self.default_provider
            .as_ref()
            .and_then(|name| self.get_provider(name))
    }

    /// List all registered provider names
    pub fn list_providers(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    /// Load providers from environment variables
    pub fn load_from_environment(&mut self) -> Result<()> {
        let mut loaded_count = 0;

        // OpenAI
        if let Ok(api_key) = std::env::var("OPENAI_API_KEY") {
            let config = ProviderConfig::new("openai".to_string(), api_key, ProviderType::OpenAI);

            // Add base_url if specified
            let config = if let Ok(base_url) = std::env::var("OPENAI_BASE_URL") {
                config.with_param("base_url".to_string(), base_url)
            } else {
                config
            };

            let provider = super::providers::openai::OpenAIProvider::new(&config);
            self.register_provider("openai".to_string(), Arc::new(provider));
            loaded_count += 1;

            // Set as default if it's the first one
            if self.default_provider.is_none() {
                self.default_provider = Some("openai".to_string());
            }
        }

        // Anthropic
        if let Ok(api_key) = std::env::var("ANTHROPIC_API_KEY") {
            let config =
                ProviderConfig::new("anthropic".to_string(), api_key, ProviderType::Anthropic);

            let provider = super::providers::anthropic::AnthropicProvider::new(&config);
            self.register_provider("anthropic".to_string(), Arc::new(provider));
            loaded_count += 1;

            // Set as default if it's the first one
            if self.default_provider.is_none() {
                self.default_provider = Some("anthropic".to_string());
            }
        }

        // Azure OpenAI
        if let (Ok(api_key), Ok(endpoint)) = (
            std::env::var("AZURE_OPENAI_API_KEY"),
            std::env::var("AZURE_OPENAI_ENDPOINT"),
        ) {
            let mut config = ProviderConfig::new("azure".to_string(), api_key, ProviderType::Azure);
            config = config.with_param("endpoint".to_string(), endpoint);

            if let Ok(api_version) = std::env::var("AZURE_OPENAI_API_VERSION") {
                config = config.with_param("api_version".to_string(), api_version);
            }

            let provider = super::providers::azure::AzureProvider::new(&config);
            self.register_provider("azure".to_string(), Arc::new(provider));
            loaded_count += 1;

            if self.default_provider.is_none() {
                self.default_provider = Some("azure".to_string());
            }
        }

        // AWS Bedrock
        if std::env::var("AWS_ACCESS_KEY_ID").is_ok()
            || std::env::var("AWS_PROFILE").is_ok()
            || std::env::var("AWS_REGION").is_ok()
        {
            let config = ProviderConfig::new(
                "bedrock".to_string(),
                "aws_credentials".to_string(), // Not used for AWS, credentials from environment
                ProviderType::Bedrock,
            );

            // Add region if specified
            let config = if let Ok(region) = std::env::var("AWS_REGION") {
                config.with_param("region".to_string(), region)
            } else {
                config.with_param("region".to_string(), "us-east-1".to_string())
            };

            let provider = super::providers::bedrock::BedrockProvider::new(&config);
            self.register_provider("bedrock".to_string(), Arc::new(provider));
            loaded_count += 1;

            if self.default_provider.is_none() {
                self.default_provider = Some("bedrock".to_string());
            }
        }

        // Google Vertex AI (using Gemini API)
        if let Ok(api_key) = std::env::var("GEMINI_API_KEY") {
            let mut config = ProviderConfig::new(
                "vertexai".to_string(),
                api_key, // Use Gemini API key instead of GCP credentials
                ProviderType::VertexAI,
            );

            // Project ID is optional for public Gemini API but keep for compatibility
            if let Ok(project_id) = std::env::var("GOOGLE_CLOUD_PROJECT") {
                config = config.with_param("project_id".to_string(), project_id);
            }

            if let Ok(location) = std::env::var("GOOGLE_CLOUD_LOCATION") {
                config = config.with_param("location".to_string(), location);
            } else {
                config = config.with_param("location".to_string(), "us-central1".to_string());
            }

            let provider = super::providers::vertexai::VertexAIProvider::new(&config);
            self.register_provider("vertexai".to_string(), Arc::new(provider));
            loaded_count += 1;

            if self.default_provider.is_none() {
                self.default_provider = Some("vertexai".to_string());
            }
        }

        // OpenRouter
        if let Ok(api_key) = std::env::var("OPENROUTER_API_KEY") {
            let mut config =
                ProviderConfig::new("openrouter".to_string(), api_key, ProviderType::OpenRouter);

            // Add optional base URL override
            if let Ok(base_url) = std::env::var("OPENROUTER_BASE_URL") {
                config = config.with_param("base_url".to_string(), base_url);
            }

            // Add optional default models
            if let Ok(models) = std::env::var("OPENROUTER_DEFAULT_MODELS") {
                config = config.with_param("default_models".to_string(), models);
            }

            // Add optional app name for attribution
            if let Ok(app_name) = std::env::var("OPENROUTER_APP_NAME") {
                config = config.with_param("app_name".to_string(), app_name);
            }

            // Add optional referer for tracking
            if let Ok(referer) = std::env::var("OPENROUTER_REFERER") {
                config = config.with_param("referer".to_string(), referer);
            }

            // Add optional routing strategy
            if let Ok(route) = std::env::var("OPENROUTER_ROUTE") {
                config = config.with_param("route".to_string(), route);
            }

            let provider = super::providers::openrouter::OpenRouterProvider::new(&config);
            self.register_provider("openrouter".to_string(), Arc::new(provider));
            loaded_count += 1;

            if self.default_provider.is_none() {
                self.default_provider = Some("openrouter".to_string());
            }
        }

        if loaded_count == 0 {
            tracing::warn!(
                "No LLM providers loaded from environment. Set API keys in environment variables."
            );
            return Err(SdkError::Other(anyhow::anyhow!(
                "No LLM providers available"
            )));
        } else {
            tracing::info!("Loaded {} LLM providers from environment", loaded_count);
        }

        Ok(())
    }

    /// Check health of all providers
    pub async fn health_check(&self) -> HashMap<String, Result<()>> {
        let mut results = HashMap::new();

        for (name, provider) in &self.providers {
            let result = provider.health_check().await;
            results.insert(name.clone(), result);
        }

        results
    }

    /// Remove a provider
    pub fn remove_provider(&mut self, name: &str) -> Option<Arc<dyn Provider>> {
        let provider = self.providers.remove(name);

        // Clear default if it was the removed provider
        if self.default_provider.as_ref() == Some(&name.to_string()) {
            self.default_provider = None;
        }

        provider
    }

    /// Clear all providers
    pub fn clear(&mut self) {
        self.providers.clear();
        self.default_provider = None;
    }

    /// Check if a provider exists
    pub fn has_provider(&self, name: &str) -> bool {
        self.providers.contains_key(name)
    }

    /// Get provider count
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }
}

impl Default for LlmRegistry {
    fn default() -> Self {
        Self::new()
    }
}
