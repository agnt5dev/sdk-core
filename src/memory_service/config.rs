//! Runtime configuration and factory wiring for semantic memory.

use std::sync::Arc;

#[cfg(feature = "libsql-memory")]
use std::path::Path;

use crate::error::{Result, SdkError};
use crate::lm::Embedder;
use crate::memory_service::{
    DeleteMemoryRequest, MemoryRecord, MemorySearchResult, MemoryService, SaveMemoryRequest,
    SearchMemoryRequest,
};

#[cfg(feature = "libsql-memory")]
use crate::memory_service::providers::libsql::{LibSqlMemoryConfig, LibSqlMemoryProvider};
#[cfg(feature = "libsql-memory")]
use crate::memory_service::VectorMemoryProvider;

const DEFAULT_MEMORY_DB_PATH: &str = ".agnt5/memory.db";
const DEFAULT_SEARCH_LIMIT: u32 = 5;

/// Memory backend selected by runtime configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryProviderKind {
    Disabled,
    LibSql,
}

impl MemoryProviderKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryProviderKind::Disabled => "disabled",
            MemoryProviderKind::LibSql => "libsql",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "libsql" => Ok(MemoryProviderKind::LibSql),
            "disabled" | "none" | "off" => Ok(MemoryProviderKind::Disabled),
            other => Err(configuration_error(
                "AGNT5_MEMORY_PROVIDER",
                format!("unsupported memory provider `{other}`; expected `libsql` or `disabled`"),
            )),
        }
    }
}

impl std::fmt::Display for MemoryProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Failure behavior for memory operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryFailurePolicy {
    /// Memory is opportunistic: disabled/unavailable memory returns empty/None.
    BestEffort,
    /// Memory is required for this runtime and unavailable memory fails calls.
    RequireMemoryOrFail,
}

impl MemoryFailurePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryFailurePolicy::BestEffort => "best_effort",
            MemoryFailurePolicy::RequireMemoryOrFail => "require_memory_or_fail",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "best_effort" | "best-effort" => Ok(MemoryFailurePolicy::BestEffort),
            "require_memory_or_fail" | "require-memory-or-fail" => {
                Ok(MemoryFailurePolicy::RequireMemoryOrFail)
            }
            other => Err(configuration_error(
                "AGNT5_MEMORY_FAILURE_POLICY",
                format!(
                    "unsupported memory failure policy `{other}`; expected `best_effort` or `require_memory_or_fail`"
                ),
            )),
        }
    }
}

impl std::fmt::Display for MemoryFailurePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Runtime memory configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRuntimeConfig {
    pub enabled: bool,
    pub provider: MemoryProviderKind,
    pub libsql_path: String,
    pub default_search_limit: u32,
    pub min_score: Option<f32>,
    pub failure_policy: MemoryFailurePolicy,
}

impl Default for MemoryRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: MemoryProviderKind::LibSql,
            libsql_path: DEFAULT_MEMORY_DB_PATH.to_string(),
            default_search_limit: DEFAULT_SEARCH_LIMIT,
            min_score: None,
            failure_policy: MemoryFailurePolicy::BestEffort,
        }
    }
}

impl MemoryRuntimeConfig {
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();
        config.enabled = parse_env_bool("AGNT5_MEMORY_ENABLED", config.enabled)?;
        config.provider = match std::env::var("AGNT5_MEMORY_PROVIDER") {
            Ok(raw) => MemoryProviderKind::parse(&raw)?,
            Err(_) => config.provider,
        };
        if config.provider == MemoryProviderKind::Disabled {
            config.enabled = false;
        }

        if let Ok(path) = std::env::var("AGNT5_MEMORY_DB_PATH") {
            config.libsql_path = path;
        }
        config.default_search_limit =
            parse_env_u32("AGNT5_MEMORY_SEARCH_TOP_K", config.default_search_limit)?;
        config.min_score = parse_env_optional_f32("AGNT5_MEMORY_MIN_SCORE")?;
        config.failure_policy = match std::env::var("AGNT5_MEMORY_FAILURE_POLICY") {
            Ok(raw) => MemoryFailurePolicy::parse(&raw)?,
            Err(_) => config.failure_policy,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn enabled_libsql(path: impl Into<String>) -> Self {
        Self {
            enabled: true,
            provider: MemoryProviderKind::LibSql,
            libsql_path: path.into(),
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.default_search_limit == 0 {
            return Err(configuration_error(
                "AGNT5_MEMORY_SEARCH_TOP_K",
                "memory search top-k must be greater than zero",
            ));
        }
        if let Some(min_score) = self.min_score {
            if !(0.0..=1.0).contains(&min_score) {
                return Err(configuration_error(
                    "AGNT5_MEMORY_MIN_SCORE",
                    "memory min score must be between 0.0 and 1.0",
                ));
            }
        }
        if self.enabled
            && self.provider == MemoryProviderKind::LibSql
            && self.libsql_path.trim().is_empty()
        {
            return Err(configuration_error(
                "AGNT5_MEMORY_DB_PATH",
                "libSQL memory path must not be empty",
            ));
        }
        Ok(())
    }
}

/// Configured memory runtime used by SDK bindings.
#[derive(Clone)]
pub struct MemoryRuntime {
    config: MemoryRuntimeConfig,
    service: Option<Arc<MemoryService>>,
    unavailable_reason: Option<String>,
}

impl MemoryRuntime {
    pub fn disabled(config: MemoryRuntimeConfig, reason: impl Into<String>) -> Self {
        Self {
            config,
            service: None,
            unavailable_reason: Some(reason.into()),
        }
    }

    pub fn available(config: MemoryRuntimeConfig, service: Arc<MemoryService>) -> Self {
        Self {
            config,
            service: Some(service),
            unavailable_reason: None,
        }
    }

    pub fn config(&self) -> &MemoryRuntimeConfig {
        &self.config
    }

    pub fn service(&self) -> Option<Arc<MemoryService>> {
        self.service.clone()
    }

    pub fn is_available(&self) -> bool {
        self.service.is_some()
    }

    pub fn unavailable_reason(&self) -> Option<&str> {
        self.unavailable_reason.as_deref()
    }

    pub fn provider_name(&self) -> Option<&'static str> {
        self.service.as_ref().map(|service| service.provider_name())
    }

    pub async fn save_memory(&self, request: SaveMemoryRequest) -> Result<Option<MemoryRecord>> {
        let Some(service) = self.service.as_ref() else {
            return self.unavailable_save_result();
        };

        match service.save_memory(request).await {
            Ok(record) => Ok(Some(record)),
            Err(error) if self.config.failure_policy == MemoryFailurePolicy::BestEffort => {
                tracing::warn!(error = %error, "best-effort memory save failed");
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn search_memory(
        &self,
        mut request: SearchMemoryRequest,
    ) -> Result<Vec<MemorySearchResult>> {
        let Some(service) = self.service.as_ref() else {
            return self.unavailable_search_result();
        };

        if request.limit == 0 {
            request.limit = self.config.default_search_limit;
        }
        if request.min_score.is_none() {
            request.min_score = self.config.min_score;
        }

        match service.search_memory(request).await {
            Ok(results) => Ok(results),
            Err(error) if self.config.failure_policy == MemoryFailurePolicy::BestEffort => {
                tracing::warn!(error = %error, "best-effort memory search failed");
                Ok(Vec::new())
            }
            Err(error) => Err(error),
        }
    }

    pub async fn delete_memory(&self, request: DeleteMemoryRequest) -> Result<u64> {
        let Some(service) = self.service.as_ref() else {
            return self.unavailable_delete_result();
        };

        match service.delete_memory(request).await {
            Ok(deleted) => Ok(deleted),
            Err(error) if self.config.failure_policy == MemoryFailurePolicy::BestEffort => {
                tracing::warn!(error = %error, "best-effort memory delete failed");
                Ok(0)
            }
            Err(error) => Err(error),
        }
    }

    fn unavailable_save_result(&self) -> Result<Option<MemoryRecord>> {
        if self.config.failure_policy == MemoryFailurePolicy::BestEffort {
            Ok(None)
        } else {
            Err(memory_unavailable_error(self.unavailable_reason()))
        }
    }

    fn unavailable_search_result(&self) -> Result<Vec<MemorySearchResult>> {
        if self.config.failure_policy == MemoryFailurePolicy::BestEffort {
            Ok(Vec::new())
        } else {
            Err(memory_unavailable_error(self.unavailable_reason()))
        }
    }

    fn unavailable_delete_result(&self) -> Result<u64> {
        if self.config.failure_policy == MemoryFailurePolicy::BestEffort {
            Ok(0)
        } else {
            Err(memory_unavailable_error(self.unavailable_reason()))
        }
    }
}

pub struct MemoryServiceFactory;

impl MemoryServiceFactory {
    pub async fn from_env(embedder: Arc<dyn Embedder>) -> Result<MemoryRuntime> {
        let config = MemoryRuntimeConfig::from_env()?;
        Self::from_config(config, embedder).await
    }

    pub async fn from_config(
        config: MemoryRuntimeConfig,
        embedder: Arc<dyn Embedder>,
    ) -> Result<MemoryRuntime> {
        config.validate()?;

        if !config.enabled || config.provider == MemoryProviderKind::Disabled {
            return Ok(MemoryRuntime::disabled(
                config,
                "memory disabled by configuration",
            ));
        }

        if embedder.dimension() == 0 {
            return Err(configuration_error(
                "embedding_dim",
                "memory embedder dimension must be greater than zero",
            ));
        }

        match config.provider {
            MemoryProviderKind::Disabled => Ok(MemoryRuntime::disabled(
                config,
                "memory disabled by configuration",
            )),
            MemoryProviderKind::LibSql => build_libsql_runtime(config, embedder).await,
        }
    }
}

async fn build_libsql_runtime(
    config: MemoryRuntimeConfig,
    embedder: Arc<dyn Embedder>,
) -> Result<MemoryRuntime> {
    let result = build_libsql_service(&config, embedder).await;
    match result {
        Ok(service) => Ok(MemoryRuntime::available(config, service)),
        Err(error) if config.failure_policy == MemoryFailurePolicy::BestEffort => {
            tracing::warn!(error = %error, "memory provider unavailable; disabling best-effort memory");
            Ok(MemoryRuntime::disabled(config, error.to_string()))
        }
        Err(error) => Err(error),
    }
}

#[cfg(feature = "libsql-memory")]
async fn build_libsql_service(
    config: &MemoryRuntimeConfig,
    embedder: Arc<dyn Embedder>,
) -> Result<Arc<MemoryService>> {
    ensure_parent_dir(&config.libsql_path)?;
    let provider = LibSqlMemoryProvider::new(LibSqlMemoryConfig::new(
        &config.libsql_path,
        embedder.dimension(),
    ))
    .await?;
    provider.health_check().await?;
    Ok(Arc::new(MemoryService::new(embedder, Arc::new(provider))))
}

#[cfg(not(feature = "libsql-memory"))]
async fn build_libsql_service(
    _config: &MemoryRuntimeConfig,
    _embedder: Arc<dyn Embedder>,
) -> Result<Arc<MemoryService>> {
    Err(configuration_error(
        "AGNT5_MEMORY_PROVIDER",
        "libSQL memory provider requires the `libsql-memory` feature",
    ))
}

#[cfg(feature = "libsql-memory")]
fn ensure_parent_dir(path: &str) -> Result<()> {
    if path == ":memory:" {
        return Ok(());
    }
    let Some(parent) = Path::new(path).parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(|error| {
        configuration_error(
            "AGNT5_MEMORY_DB_PATH",
            format!("create memory database parent directory: {error}"),
        )
    })
}

fn parse_env_bool(name: &'static str, default: bool) -> Result<bool> {
    match std::env::var(name) {
        Ok(raw) => parse_bool(name, &raw),
        Err(_) => Ok(default),
    }
}

fn parse_bool(name: &'static str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(configuration_error(
            name,
            format!("invalid boolean value `{other}`"),
        )),
    }
}

fn parse_env_u32(name: &'static str, default: u32) -> Result<u32> {
    match std::env::var(name) {
        Ok(raw) => raw.trim().parse::<u32>().map_err(|error| {
            configuration_error(name, format!("invalid unsigned integer value: {error}"))
        }),
        Err(_) => Ok(default),
    }
}

fn parse_env_optional_f32(name: &'static str) -> Result<Option<f32>> {
    match std::env::var(name) {
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .trim()
            .parse::<f32>()
            .map(Some)
            .map_err(|error| configuration_error(name, format!("invalid float value: {error}"))),
        Err(_) => Ok(None),
    }
}

fn configuration_error(field: impl Into<String>, message: impl Into<String>) -> SdkError {
    SdkError::Configuration {
        message: message.into(),
        field: Some(field.into()),
    }
}

fn memory_unavailable_error(reason: Option<&str>) -> SdkError {
    SdkError::Unavailable {
        message: reason
            .unwrap_or("memory service is unavailable")
            .to_string(),
        service: Some("memory".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::memory_service::tests::TestEmbedder;
    use crate::memory_service::{MemoryScope, MemoryScopeFilter};

    struct ZeroDimEmbedder;

    #[async_trait]
    impl Embedder for ZeroDimEmbedder {
        async fn embed(&self, _text: &str) -> Result<Vec<f32>> {
            Ok(Vec::new())
        }

        fn dimension(&self) -> u32 {
            0
        }

        fn provider_name(&self) -> &'static str {
            "zero"
        }

        fn model_name(&self) -> &str {
            "zero-dim"
        }
    }

    fn search_request() -> SearchMemoryRequest {
        SearchMemoryRequest {
            tenant_id: "tenant-a".to_string(),
            deployment_id: "dep-1".to_string(),
            query: "query".to_string(),
            scope_filters: vec![MemoryScopeFilter::new(MemoryScope::User, "user-a")],
            kinds: vec![],
            limit: 0,
            min_score: None,
        }
    }

    fn save_request() -> SaveMemoryRequest {
        SaveMemoryRequest {
            id: None,
            tenant_id: "tenant-a".to_string(),
            deployment_id: "dep-1".to_string(),
            scope: MemoryScope::User,
            scope_id: "user-a".to_string(),
            kind: "preference".to_string(),
            content: "allowed user memory".to_string(),
            metadata: json!({}),
            source_session_id: Some("session-a".to_string()),
            source_run_id: None,
            source_event_id: None,
        }
    }

    #[tokio::test]
    async fn disabled_best_effort_memory_returns_empty_results() -> Result<()> {
        let config = MemoryRuntimeConfig::default();
        let runtime = MemoryServiceFactory::from_config(config, Arc::new(TestEmbedder)).await?;

        assert!(!runtime.is_available());
        assert_eq!(
            runtime.unavailable_reason(),
            Some("memory disabled by configuration")
        );
        assert!(runtime.search_memory(search_request()).await?.is_empty());
        assert!(runtime.save_memory(save_request()).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn disabled_required_memory_fails_calls() -> Result<()> {
        let config = MemoryRuntimeConfig {
            failure_policy: MemoryFailurePolicy::RequireMemoryOrFail,
            ..MemoryRuntimeConfig::default()
        };
        let runtime = MemoryServiceFactory::from_config(config, Arc::new(TestEmbedder)).await?;

        let err = runtime.search_memory(search_request()).await.unwrap_err();
        assert!(matches!(err, SdkError::Unavailable { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn enabled_memory_requires_non_zero_embedding_dimension() {
        let config = MemoryRuntimeConfig::enabled_libsql(":memory:");
        let err = match MemoryServiceFactory::from_config(config, Arc::new(ZeroDimEmbedder)).await {
            Ok(_) => panic!("zero-dimension embedder should fail configuration"),
            Err(err) => err,
        };

        assert!(matches!(err, SdkError::Configuration { .. }));
        assert!(err.to_string().contains("dimension"));
    }

    #[test]
    fn config_validates_score_and_limit() {
        let top_k_error = MemoryRuntimeConfig {
            enabled: true,
            default_search_limit: 0,
            ..MemoryRuntimeConfig::enabled_libsql(":memory:")
        }
        .validate()
        .unwrap_err();
        assert!(top_k_error.to_string().contains("top-k"));

        let score_error = MemoryRuntimeConfig {
            enabled: true,
            min_score: Some(1.5),
            ..MemoryRuntimeConfig::enabled_libsql(":memory:")
        }
        .validate()
        .unwrap_err();
        assert!(score_error.to_string().contains("min score"));
    }

    #[cfg(feature = "libsql-memory")]
    #[tokio::test]
    async fn libsql_provider_selected_by_config() -> Result<()> {
        let config = MemoryRuntimeConfig::enabled_libsql(":memory:");
        let runtime = MemoryServiceFactory::from_config(config, Arc::new(TestEmbedder)).await?;

        assert!(runtime.is_available());
        assert_eq!(runtime.provider_name(), Some("libsql"));

        runtime.save_memory(save_request()).await?;
        let hits = runtime.search_memory(search_request()).await?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record.scope_id, "user-a");

        Ok(())
    }

    #[cfg(feature = "libsql-memory")]
    #[tokio::test]
    async fn best_effort_provider_failure_disables_memory() -> Result<()> {
        let parent_file =
            std::env::temp_dir().join(format!("agnt5-memory-parent-{}", uuid::Uuid::new_v4()));
        std::fs::write(&parent_file, b"not a directory").unwrap();
        let db_path = parent_file.join("memory.db").to_string_lossy().to_string();

        let config = MemoryRuntimeConfig::enabled_libsql(db_path);
        let runtime = MemoryServiceFactory::from_config(config, Arc::new(TestEmbedder)).await?;

        assert!(!runtime.is_available());
        assert!(runtime.unavailable_reason().is_some());
        assert!(runtime.search_memory(search_request()).await?.is_empty());

        let _ = std::fs::remove_file(parent_file);
        Ok(())
    }

    #[cfg(not(feature = "libsql-memory"))]
    #[tokio::test]
    async fn best_effort_missing_provider_feature_disables_memory() -> Result<()> {
        let config = MemoryRuntimeConfig::enabled_libsql(":memory:");
        let runtime = MemoryServiceFactory::from_config(config, Arc::new(TestEmbedder)).await?;

        assert!(!runtime.is_available());
        assert!(runtime
            .unavailable_reason()
            .is_some_and(|reason| reason.contains("libsql-memory")));
        assert!(runtime.search_memory(search_request()).await?.is_empty());
        Ok(())
    }
}
