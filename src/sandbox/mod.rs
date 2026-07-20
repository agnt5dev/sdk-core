//! Sandbox execution for AGNT5 SDK-Core.
//!
//! Provides a backend-agnostic interface for sandboxed code execution and
//! workspace file operations. Two backends are available:
//!
//! - **WasmSandbox** (Level 2): Embedded Wasmtime + WASI execution. JS/Wasm-first,
//!   zero infrastructure required. Behind the `wasm-sandbox` feature flag.
//! - **RemoteSandbox**: HTTP client to any Level 4/5 sandbox provider (E2B, Daytona,
//!   self-hosted, or the existing Go/K8s sandbox).
//!
//! # Trait Design
//!
//! The sandbox interface is split into two composable traits:
//!
//! - [`SandboxExecutor`]: Code execution and health checks.
//! - [`SandboxWorkspace`]: File read/write/delete/list operations.
//!
//! Any type implementing both traits automatically implements [`SandboxBackend`]
//! via a blanket impl. This separation keeps backends with partial capabilities
//! (e.g., WasmSandbox has no shell/process model) clean.
//!
//! Operations that only make sense with a real OS (shell commands, git, preview URLs)
//! are inherent methods on [`RemoteSandbox`](providers::remote::RemoteSandbox), not
//! part of the universal trait.

pub mod providers;
pub mod types;

pub use providers::remote::{RemoteSandbox, RemoteSandboxConfig, SandboxAuth};
pub use types::*;

#[cfg(feature = "wasm-sandbox")]
pub use providers::wasm::{WasmSandbox, WasmSandboxConfig};

use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

// ── Core Traits ─────────────────────────────────────────────────

/// Code execution — every sandbox backend must support this.
#[async_trait]
pub trait SandboxExecutor: Send + Sync {
    /// Identifies the backend type (Wasm or Remote).
    fn backend_kind(&self) -> SandboxBackendKind;

    /// Returns the capabilities of this backend. Callers should check
    /// capabilities before invoking operations to fail early.
    fn capabilities(&self) -> &SandboxCapabilities;

    /// Execute code in the sandbox.
    async fn execute_code(&self, req: ExecuteCodeRequest) -> Result<ExecuteCodeResult>;

    /// Check sandbox health.
    async fn health(&self) -> Result<SandboxHealthResult>;
}

/// Workspace file operations — every sandbox backend must support this.
#[async_trait]
pub trait SandboxWorkspace: Send + Sync {
    /// Write a file to the sandbox workspace.
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult>;

    /// Read a file from the sandbox workspace.
    async fn read_file(&self, path: &str) -> Result<ReadFileResult>;

    /// Delete a file or directory from the sandbox workspace.
    async fn delete_file(&self, path: &str, recursive: bool) -> Result<bool>;

    /// List files in a directory within the sandbox workspace.
    async fn list_files(&self, path: &str, recursive: bool) -> Result<ListFilesResult>;
}

/// Full sandbox backend = executor + workspace.
///
/// This trait is automatically implemented for any type that implements
/// both [`SandboxExecutor`] and [`SandboxWorkspace`].
pub trait SandboxBackend: SandboxExecutor + SandboxWorkspace {}

impl<T: SandboxExecutor + SandboxWorkspace> SandboxBackend for T {}

/// Control plane for a managed sandbox provider.
///
/// While [`SandboxBackend`] is the data plane for a *running* sandbox,
/// `SandboxProvider` manages sandbox lifecycle: provisioning new instances,
/// reconnecting to existing ones, and tearing them down.
///
/// Provider-specific extras (e.g., E2B preview URLs, Daytona git operations)
/// are inherent methods on the concrete handle types, mirroring how
/// `run_command` is inherent on [`RemoteSandbox`] rather than part of the
/// universal trait.
#[async_trait]
pub trait SandboxProvider: Send + Sync {
    /// Stable provider name used as the registry key.
    fn name(&self) -> &'static str;

    /// Provision a new sandbox and return a connected backend handle.
    async fn create_sandbox(&self, opts: CreateSandboxOptions) -> Result<Arc<dyn SandboxBackend>>;

    /// Connect to an existing sandbox by provider-native ID.
    async fn connect_sandbox(&self, sandbox_id: &str) -> Result<Arc<dyn SandboxBackend>>;

    /// Destroy a sandbox. Returns `true` if the provider confirmed deletion.
    async fn destroy_sandbox(&self, sandbox_id: &str) -> Result<bool>;

    /// List sandboxes visible to the configured credentials.
    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>>;
}

// ── Registry ────────────────────────────────────────────────────

/// Registry for managing sandbox backends.
///
/// Supports explicit backend selection. Auto-detection from environment
/// variables is available but never silently falls back from a misconfigured
/// remote to wasm — it only auto-selects when no backend is specified at all.
pub struct SandboxRegistry {
    backends: HashMap<String, Arc<dyn SandboxBackend>>,
    providers: HashMap<String, Arc<dyn SandboxProvider>>,
    default_backend: Option<String>,
}

impl SandboxRegistry {
    pub fn new() -> Self {
        Self {
            backends: HashMap::new(),
            providers: HashMap::new(),
            default_backend: None,
        }
    }

    /// Register a named backend.
    pub fn register(&mut self, name: String, backend: Arc<dyn SandboxBackend>) {
        if self.default_backend.is_none() {
            self.default_backend = Some(name.clone());
        }
        self.backends.insert(name, backend);
    }

    /// Get a backend by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn SandboxBackend>> {
        self.backends.get(name).cloned()
    }

    /// Get the default backend.
    pub fn default_backend(&self) -> Option<Arc<dyn SandboxBackend>> {
        self.default_backend
            .as_ref()
            .and_then(|name| self.backends.get(name))
            .cloned()
    }

    /// Set which registered backend is the default.
    pub fn set_default(&mut self, name: &str) -> Result<()> {
        if !self.backends.contains_key(name) {
            return Err(crate::error::SdkError::Configuration {
                message: format!("sandbox backend '{}' is not registered", name),
                field: Some("default_backend".to_string()),
            });
        }
        self.default_backend = Some(name.to_string());
        Ok(())
    }

    /// List registered backend names.
    pub fn list_backends(&self) -> Vec<&str> {
        self.backends.keys().map(|s| s.as_str()).collect()
    }

    /// Register a sandbox provider control plane under its [`SandboxProvider::name`].
    pub fn register_provider(&mut self, provider: Arc<dyn SandboxProvider>) {
        self.providers.insert(provider.name().to_string(), provider);
    }

    /// Get a registered provider by name.
    pub fn get_provider(&self, name: &str) -> Option<Arc<dyn SandboxProvider>> {
        self.providers.get(name).cloned()
    }

    /// List registered provider names.
    pub fn list_providers(&self) -> Vec<&str> {
        self.providers.keys().map(|s| s.as_str()).collect()
    }

    /// Auto-detect backends from environment variables.
    ///
    /// - If `AGNT5_SANDBOX_ENDPOINT` is set, registers a `RemoteSandbox` as "remote".
    /// - If the `wasm-sandbox` feature is enabled and no remote was configured,
    ///   registers a `WasmSandbox` as "wasm".
    ///
    /// This method does NOT silently fall back from remote to wasm. If
    /// `AGNT5_SANDBOX_ENDPOINT` is set but invalid, it returns an error
    /// rather than falling back.
    pub fn load_from_environment(&mut self) -> Result<()> {
        if let Ok(endpoint) = std::env::var("AGNT5_SANDBOX_ENDPOINT") {
            let sandbox_id =
                std::env::var("AGNT5_SANDBOX_ID").unwrap_or_else(|_| "default".to_string());

            let auth = if let Ok(key) = std::env::var("AGNT5_SANDBOX_API_KEY") {
                SandboxAuth::ApiKey(key)
            } else if let Ok(token) = std::env::var("AGNT5_SANDBOX_BEARER_TOKEN") {
                SandboxAuth::BearerToken(token)
            } else {
                SandboxAuth::None
            };

            let timeout_secs: u64 = std::env::var("AGNT5_SANDBOX_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300);

            let api_prefix = std::env::var("AGNT5_SANDBOX_API_PREFIX").unwrap_or_default();

            let config = RemoteSandboxConfig {
                endpoint,
                sandbox_id,
                auth,
                timeout: std::time::Duration::from_secs(timeout_secs),
                api_prefix,
            };
            let remote = RemoteSandbox::new(config)?;
            self.register("remote".to_string(), Arc::new(remote));
            return Ok(());
        }

        #[cfg(feature = "wasm-sandbox")]
        {
            let config = WasmSandboxConfig::default();
            let wasm = WasmSandbox::new(config)?;
            self.register("wasm".to_string(), Arc::new(wasm));
        }

        Ok(())
    }
}

impl Default for SandboxRegistry {
    fn default() -> Self {
        Self::new()
    }
}
