// OpenRouter native provider implementation
use async_trait::async_trait;
use reqwest::Client;
use std::time::Duration;

use crate::error::{Result, SdkError};
use crate::llm::{Provider, ProviderConfig, ProviderType};
use crate::llm::models::{
    ChatCompletionRequest, ChatCompletionResponse,
    CompletionRequest, CompletionResponse, EmbeddingsRequest, EmbeddingsResponse,
    Choice
};
use super::types::{
    RouteStrategy, ProviderPreferences, Transform, OpenRouterModel, GenerationInfo,
    OpenRouterLimits, OpenRouterChatRequest, OpenRouterChatResponse, ResponseFormat
};

/// OpenRouter provider with native API support
pub struct OpenRouterProvider {
    config: ProviderConfig,
    http_client: Client,
    base_url: String,

    // OpenRouter-specific configuration
    default_models: Vec<String>,
    route: Option<RouteStrategy>,
    provider_preferences: Option<ProviderPreferences>,
    transforms: Vec<Transform>,
    app_name: Option<String>,
    referer: Option<String>,
}

impl OpenRouterProvider {
    pub fn new(config: &ProviderConfig) -> Self {
        let timeout = Duration::from_secs(60);
        let http_client = Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to create HTTP client");

        let base_url = config
            .get_param("base_url")
            .cloned()
            .unwrap_or_else(|| "https://openrouter.ai/api/v1".to_string());

        let default_models = config
            .get_param("default_models")
            .map(|models| models.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_else(|| vec!["anthropic/claude-3-haiku".to_string()]);

        let route = config
            .get_param("route")
            .and_then(|r| match r.as_str() {
                "fallback" => Some(RouteStrategy::Fallback),
                _ => None,
            });

        let app_name = config.get_param("app_name").cloned();
        let referer = config.get_param("referer").cloned();

        Self {
            config: config.clone(),
            http_client,
            base_url,
            default_models,
            route,
            provider_preferences: None,
            transforms: Vec::new(),
            app_name,
            referer,
        }
    }

    /// Set multiple models for fallback routing
    pub fn with_models(mut self, models: Vec<String>) -> Self {
        self.default_models = models;
        self
    }

    /// Set routing strategy
    pub fn with_route(mut self, route: RouteStrategy) -> Self {
        self.route = Some(route);
        self
    }

    /// Set provider preferences
    pub fn with_provider_preferences(mut self, preferences: ProviderPreferences) -> Self {
        self.provider_preferences = Some(preferences);
        self
    }

    /// Add transforms
    pub fn with_transforms(mut self, transforms: Vec<Transform>) -> Self {
        self.transforms = transforms;
        self
    }

    /// Set app name for attribution
    pub fn with_app_name(mut self, app_name: String) -> Self {
        self.app_name = Some(app_name);
        self
    }

    /// Set referer for tracking
    pub fn with_referer(mut self, referer: String) -> Self {
        self.referer = Some(referer);
        self
    }

    /// Get available models with pricing information
    pub async fn list_models(&self) -> Result<Vec<OpenRouterModel>> {
        let url = format!("{}/models", self.base_url);

        let mut request = self.http_client
            .get(&url)
            .bearer_auth(&self.config.api_key);

        // Add optional headers
        if let Some(ref referer) = self.referer {
            request = request.header("HTTP-Referer", referer);
        }
        if let Some(ref app_name) = self.app_name {
            request = request.header("X-Title", app_name);
        }

        let response = request
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(SdkError::Other(anyhow::anyhow!(
                "OpenRouter API error: {}", error_text
            )));
        }

        #[derive(serde::Deserialize)]
        struct ModelsResponse {
            data: Vec<OpenRouterModel>,
        }

        let models_response: ModelsResponse = response
            .json()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to parse response: {}", e)))?;

        Ok(models_response.data)
    }

    /// Get generation information for cost tracking
    pub async fn get_generation_info(&self, generation_id: &str) -> Result<GenerationInfo> {
        let url = format!("{}/generation?id={}", self.base_url, generation_id);

        let mut request = self.http_client
            .get(&url)
            .bearer_auth(&self.config.api_key);

        // Add optional headers
        if let Some(ref referer) = self.referer {
            request = request.header("HTTP-Referer", referer);
        }
        if let Some(ref app_name) = self.app_name {
            request = request.header("X-Title", app_name);
        }

        let response = request
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(SdkError::Other(anyhow::anyhow!(
                "OpenRouter API error: {}", error_text
            )));
        }

        let generation_info: GenerationInfo = response
            .json()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to parse response: {}", e)))?;

        Ok(generation_info)
    }

    /// Get current limits and quotas
    pub async fn get_limits(&self) -> Result<OpenRouterLimits> {
        let url = format!("{}/auth/key", self.base_url);

        let mut request = self.http_client
            .get(&url)
            .bearer_auth(&self.config.api_key);

        // Add optional headers
        if let Some(ref referer) = self.referer {
            request = request.header("HTTP-Referer", referer);
        }
        if let Some(ref app_name) = self.app_name {
            request = request.header("X-Title", app_name);
        }

        let response = request
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(SdkError::Other(anyhow::anyhow!(
                "OpenRouter API error: {}", error_text
            )));
        }

        #[derive(serde::Deserialize)]
        struct KeyInfoResponse {
            data: OpenRouterLimits,
        }

        let key_info: KeyInfoResponse = response
            .json()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to parse response: {}", e)))?;

        Ok(key_info.data)
    }

    /// Build OpenRouter-specific chat request
    fn build_chat_request(&self, request: ChatCompletionRequest) -> OpenRouterChatRequest {
        let primary_model = request.model.clone();

        // Use multiple models if configured for fallback
        let models = if self.default_models.len() > 1 {
            Some(self.default_models.clone())
        } else {
            None
        };

        let response_format = request.response_format.map(|rf| ResponseFormat {
            format_type: rf.r#type,
            schema: None, // JSON schema not supported yet, only format type
        });

        OpenRouterChatRequest {
            model: primary_model,
            models,
            messages: request.messages,
            max_tokens: request.max_tokens.or(request.max_completion_tokens),
            temperature: request.temperature,
            top_p: request.top_p,
            top_k: None, // OpenRouter uses top_k instead of top_logprobs
            frequency_penalty: request.frequency_penalty,
            presence_penalty: request.presence_penalty,
            repetition_penalty: None,
            stop: request.stop,
            stream: request.stream,
            route: self.route.clone(),
            provider: self.provider_preferences.clone(),
            transforms: if self.transforms.is_empty() { None } else { Some(self.transforms.clone()) },
            response_format,
            tools: request.tools,
            tool_choice: request.tool_choice,
            user: request.user,
        }
    }

    /// Convert OpenRouter response to standard format
    fn convert_response(&self, response: OpenRouterChatResponse) -> ChatCompletionResponse {
        let choices = response.choices.into_iter().map(|choice| {
            Choice {
                index: choice.index,
                message: choice.message,
                finish_reason: choice.finish_reason,
                logprobs: choice.logprobs,
            }
        }).collect();

        ChatCompletionResponse::NonStream(crate::llm::models::ChatCompletion {
            id: response.id,
            object: Some(response.object),
            created: Some(response.created),
            model: response.model,
            choices,
            usage: response.usage.unwrap_or_default(),
            system_fingerprint: None, // OpenRouter doesn't provide this
        })
    }
}

#[async_trait]
impl Provider for OpenRouterProvider {
    fn key(&self) -> String {
        self.config.key.clone()
    }

    fn r#type(&self) -> ProviderType {
        ProviderType::OpenRouter
    }

    async fn chat_completion(&self, request: ChatCompletionRequest) -> Result<ChatCompletionResponse> {
        let url = format!("{}/chat/completions", self.base_url);
        let openrouter_request = self.build_chat_request(request.clone());

        let mut http_request = self.http_client
            .post(&url)
            .bearer_auth(&self.config.api_key)
            .header("Content-Type", "application/json")
            .json(&openrouter_request);

        // Add optional headers
        if let Some(ref referer) = self.referer {
            http_request = http_request.header("HTTP-Referer", referer);
        }
        if let Some(ref app_name) = self.app_name {
            http_request = http_request.header("X-Title", app_name);
        }

        let response = http_request
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(SdkError::Other(anyhow::anyhow!(
                "OpenRouter API error: {}", error_text
            )));
        }

        // Handle streaming vs non-streaming
        if request.stream.unwrap_or(false) {
            // For now, return error for streaming - we'll implement this later
            return Err(SdkError::Other(anyhow::anyhow!(
                "Streaming not yet implemented for OpenRouter"
            )));
        } else {
            let openrouter_response: OpenRouterChatResponse = response
                .json()
                .await
                .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to parse response: {}", e)))?;

            Ok(self.convert_response(openrouter_response))
        }
    }

    async fn completion(&self, _request: CompletionRequest) -> Result<CompletionResponse> {
        // OpenRouter focuses on chat completions
        Err(SdkError::Other(anyhow::anyhow!(
            "Legacy completions not supported by OpenRouter - use chat completions instead"
        )))
    }

    async fn embeddings(&self, _request: EmbeddingsRequest) -> Result<EmbeddingsResponse> {
        // OpenRouter doesn't provide embedding endpoints
        Err(SdkError::Other(anyhow::anyhow!(
            "Embeddings not supported by OpenRouter"
        )))
    }
}