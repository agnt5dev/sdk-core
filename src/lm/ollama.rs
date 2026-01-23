use std::env;

use async_trait::async_trait;

use crate::error::Result as SdkResult;

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

const DEFAULT_BASE_URL: &str = "http://localhost:11434/v1";
const MODEL_PREFIX: &str = "ollama";

/// Configuration for the Ollama provider.
///
/// Ollama enables running large language models locally on your machine.
/// It provides an OpenAI-compatible API for easy integration.
///
/// Ollama supports various open-weight models including:
/// - Llama 2/3
/// - Mistral/Mixtral
/// - CodeLlama
/// - Phi-2/3
/// - Gemma
/// - And many more community models
#[derive(Clone, Debug)]
pub struct OllamaConfig {
    /// API key (optional for Ollama, but may be needed for remote deployments)
    pub api_key: Option<String>,
    /// Base URL for the Ollama API (default: http://localhost:11434/v1)
    pub base_url: String,
}

impl OllamaConfig {
    /// Create a new Ollama configuration with default local settings.
    ///
    /// # Example
    ///
    /// ```
    /// use agnt5_sdk_core::lm::OllamaConfig;
    ///
    /// let config = OllamaConfig::new();
    /// assert_eq!(config.base_url, "http://localhost:11434/v1");
    /// ```
    pub fn new() -> Self {
        Self {
            api_key: None,
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }

    /// Create a configuration with a specific API key (for remote/authenticated deployments).
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Create configuration from environment variables.
    ///
    /// Environment variables:
    /// - `OLLAMA_BASE_URL`: Base URL for Ollama API (optional, defaults to localhost:11434)
    /// - `OLLAMA_API_KEY`: API key if using a remote/authenticated Ollama deployment (optional)
    pub fn from_env() -> SdkResult<Self> {
        let mut config = OllamaConfig::new();

        if let Ok(base) = env::var("OLLAMA_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }

        if let Ok(api_key) = env::var("OLLAMA_API_KEY") {
            if !api_key.trim().is_empty() {
                config.api_key = Some(api_key);
            }
        }

        Ok(config)
    }
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Provider implementation for Ollama local models.
///
/// Ollama provides an OpenAI-compatible API for running models locally.
/// This provider wraps the OpenAI Chat provider with Ollama-specific configuration.
///
/// # Example
///
/// ```no_run
/// use agnt5_sdk_core::lm::{OllamaProvider, OllamaConfig, GenerateRequest};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Use default local configuration
/// let provider = OllamaProvider::new(OllamaConfig::new())?;
///
/// let response = provider.generate(
///     GenerateRequest::new("ollama/llama3.2")
///         .user_message("Write a haiku about programming")
/// ).await?;
/// println!("{}", response.text);
/// # Ok(())
/// # }
/// ```
///
/// # Running Ollama
///
/// Make sure Ollama is running locally:
/// ```bash
/// # Install Ollama (macOS/Linux)
/// curl -fsSL https://ollama.com/install.sh | sh
///
/// # Pull a model
/// ollama pull llama3.2
///
/// # Start the server (if not running)
/// ollama serve
/// ```
#[derive(Clone)]
pub struct OllamaProvider {
    inner: OpenAiChatProvider,
}

impl OllamaProvider {
    pub fn new(config: OllamaConfig) -> SdkResult<Self> {
        // Ollama doesn't require an API key for local use
        // Use a placeholder if not provided
        let api_key = config.api_key.unwrap_or_else(|| "ollama".to_string());

        let inner_config = OpenAiChatConfig::new(api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));

        let inner = OpenAiChatProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = OllamaConfig::from_env()?;
        Self::new(config)
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}

#[async_trait]
impl LanguageModel for OllamaProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}
