//! RemoteSandbox — HTTP client to any Level 4/5 sandbox provider.
//!
//! Implements [`SandboxExecutor`] + [`SandboxWorkspace`] (and therefore [`SandboxBackend`])
//! via HTTP requests to an external sandbox server. Additionally provides inherent methods
//! for operations that only make sense with a real OS: shell commands, git, preview URLs.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxExecutor, SandboxWorkspace};
use async_trait::async_trait;
use std::time::Duration;

// ── Auth ────────────────────────────────────────────────────────

/// Authentication strategy for connecting to a remote sandbox provider.
#[derive(Debug, Clone)]
pub enum SandboxAuth {
    /// No authentication.
    None,
    /// API key sent as `X-API-Key` header.
    ApiKey(String),
    /// Bearer token sent as `Authorization: Bearer <token>`.
    BearerToken(String),
    /// Custom header name and value.
    CustomHeader { name: String, value: String },
}

// ── Config ──────────────────────────────────────────────────────

/// Configuration for connecting to a remote sandbox provider.
#[derive(Debug, Clone)]
pub struct RemoteSandboxConfig {
    /// Base URL of the sandbox HTTP API (e.g., "http://10.0.1.5:4001").
    pub endpoint: String,
    /// Sandbox instance identifier.
    pub sandbox_id: String,
    /// Authentication strategy.
    pub auth: SandboxAuth,
    /// Default timeout for HTTP requests.
    pub timeout: Duration,
    /// Optional path prefix prepended to all API routes.
    /// Empty by default (external providers use root-level paths like `/execute`).
    /// Set to `/v1/sandbox` to target the AGNT5 Go platform gateway.
    /// Env: `AGNT5_SANDBOX_API_PREFIX`
    pub api_prefix: String,
}

// ── RemoteSandbox ───────────────────────────────────────────────

/// HTTP client to an external sandbox provider (Level 4/5).
///
/// Supports all 19 sandbox operations. Implements the universal
/// [`SandboxBackend`] trait for execution and file operations, plus
/// inherent methods for OS-specific operations (commands, git, preview URLs).
pub struct RemoteSandbox {
    config: RemoteSandboxConfig,
    client: reqwest::Client,
    capabilities: SandboxCapabilities,
    base_url: String,
}

impl RemoteSandbox {
    /// Create a new RemoteSandbox client.
    pub fn new(config: RemoteSandboxConfig) -> Result<Self> {
        let endpoint = config.endpoint.trim_end_matches('/');
        let prefix = config.api_prefix.trim_matches('/');
        let base_url = if prefix.is_empty() {
            endpoint.to_string()
        } else {
            format!("{}/{}", endpoint, prefix)
        };

        let mut headers = reqwest::header::HeaderMap::new();
        match &config.auth {
            SandboxAuth::None => {}
            SandboxAuth::ApiKey(key) => {
                headers.insert(
                    "X-API-Key",
                    reqwest::header::HeaderValue::from_str(key).map_err(|e| {
                        SdkError::Configuration {
                            message: format!("invalid API key header value: {}", e),
                            field: Some("auth".to_string()),
                        }
                    })?,
                );
            }
            SandboxAuth::BearerToken(token) => {
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token)).map_err(
                        |e| SdkError::Configuration {
                            message: format!("invalid bearer token header value: {}", e),
                            field: Some("auth".to_string()),
                        },
                    )?,
                );
            }
            SandboxAuth::CustomHeader { name, value } => {
                headers.insert(
                    reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                        SdkError::Configuration {
                            message: format!("invalid custom header name: {}", e),
                            field: Some("auth".to_string()),
                        }
                    })?,
                    reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                        SdkError::Configuration {
                            message: format!("invalid custom header value: {}", e),
                            field: Some("auth".to_string()),
                        }
                    })?,
                );
            }
        }

        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .default_headers(headers)
            .build()
            .map_err(|e| SdkError::Configuration {
                message: format!("failed to create HTTP client: {}", e),
                field: None,
            })?;

        // Remote backends support everything.
        let capabilities = SandboxCapabilities {
            languages: vec![Language::Python, Language::Javascript, Language::Bash],
            supports_commands: true,
            supports_streaming: true,
            supports_git: true,
            supports_preview_url: true,
            supports_snapshots: true,
            max_execution_time_ms: 300_000,
            max_memory_bytes: 512 * 1024 * 1024,
            has_network_access: true,
        };

        Ok(Self {
            config,
            client,
            capabilities,
            base_url,
        })
    }

    /// Helper to handle HTTP response errors.
    fn map_http_error(operation: &str, status: reqwest::StatusCode, body: &str) -> SdkError {
        let code = match status.as_u16() {
            404 => ErrorCode::SandboxUnavailable,
            408 | 504 => ErrorCode::ExecutionTimeout,
            429 | 503 => ErrorCode::SandboxUnavailable,
            _ => ErrorCode::SandboxExecutionFailed,
        };
        SdkError::Sandbox {
            message: format!("HTTP {} — {}", status, body),
            operation: operation.to_string(),
            code,
        }
    }

    /// Helper to send a request and parse the response.
    async fn send_request<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<T> {
        let resp = request.send().await.map_err(|e| {
            if e.is_timeout() {
                SdkError::Sandbox {
                    message: format!("request timed out: {}", e),
                    operation: operation.to_string(),
                    code: ErrorCode::ExecutionTimeout,
                }
            } else {
                SdkError::Sandbox {
                    message: format!("request failed: {}", e),
                    operation: operation.to_string(),
                    code: ErrorCode::SandboxUnavailable,
                }
            }
        })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_http_error(operation, status, &body));
        }

        resp.json::<T>().await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to parse response: {}", e),
            operation: operation.to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })
    }

    // ── OS-specific operations (inherent, not trait) ────────────

    /// Run a shell command in the sandbox.
    ///
    /// This is an inherent method because shell commands require a real OS
    /// and are not supported by the WasmSandbox backend.
    pub async fn run_command(&self, req: RunCommandRequest) -> Result<RunCommandResult> {
        self.send_request(
            "run_command",
            self.client
                .post(format!("{}/command", self.base_url))
                .json(&req),
        )
        .await
    }

    /// Run a command with streaming output via SSE.
    pub async fn run_command_stream(&self, req: RunCommandRequest) -> Result<Vec<StreamEvent>> {
        // The trait returns a materialized event list, so collect the SSE stream.
        let resp = self
            .client
            .post(format!("{}/command/stream", self.base_url))
            .json(&req)
            .send()
            .await
            .map_err(|e| SdkError::Sandbox {
                message: format!("request failed: {}", e),
                operation: "run_command_stream".to_string(),
                code: ErrorCode::SandboxUnavailable,
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_http_error("run_command_stream", status, &body));
        }

        let body = resp.text().await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to read stream body: {}", e),
            operation: "run_command_stream".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        let mut events = Vec::new();
        for line in body.lines() {
            if let Some(data) = line.strip_prefix("data: ") {
                if let Ok(event) = serde_json::from_str::<StreamEvent>(data) {
                    events.push(event);
                }
            }
        }
        Ok(events)
    }

    /// Clone a git repository.
    pub async fn git_clone(&self, req: GitCloneRequest) -> Result<GitCloneResult> {
        self.send_request(
            "git_clone",
            self.client
                .post(format!("{}/git/clone", self.base_url))
                .json(&req),
        )
        .await
    }

    /// Get git status.
    pub async fn git_status(&self, path: Option<&str>) -> Result<GitStatusResult> {
        let mut request = self.client.get(format!("{}/git/status", self.base_url));
        if let Some(p) = path {
            request = request.query(&[("path", p)]);
        }
        self.send_request("git_status", request).await
    }

    /// Create a git commit.
    pub async fn git_commit(&self, req: GitCommitRequest) -> Result<GitCommitResult> {
        self.send_request(
            "git_commit",
            self.client
                .post(format!("{}/git/commit", self.base_url))
                .json(&req),
        )
        .await
    }

    /// Push commits to remote.
    pub async fn git_push(&self, req: GitPushRequest) -> Result<GitPushResult> {
        self.send_request(
            "git_push",
            self.client
                .post(format!("{}/git/push", self.base_url))
                .json(&req),
        )
        .await
    }

    /// Get preview URL for a web server running in the sandbox.
    pub async fn get_preview_url(&self, port: u16) -> Result<String> {
        #[derive(serde::Deserialize)]
        struct PreviewResponse {
            url: String,
        }
        let resp: PreviewResponse = self
            .send_request(
                "get_preview_url",
                self.client
                    .get(format!("{}/preview-url", self.base_url))
                    .query(&[("port", port)]),
            )
            .await?;
        Ok(resp.url)
    }

    /// Set the default execution timeout.
    pub async fn set_timeout(&self, timeout_ms: u64) -> Result<bool> {
        #[derive(serde::Serialize)]
        struct TimeoutRequest {
            timeout_ms: u64,
        }
        #[derive(serde::Deserialize)]
        struct TimeoutResponse {
            success: bool,
        }
        let resp: TimeoutResponse = self
            .send_request(
                "set_timeout",
                self.client
                    .post(format!("{}/timeout", self.base_url))
                    .json(&TimeoutRequest { timeout_ms }),
            )
            .await?;
        Ok(resp.success)
    }
}

// ── SandboxExecutor impl ────────────────────────────────────────

#[async_trait]
impl SandboxExecutor for RemoteSandbox {
    fn backend_kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::Remote
    }

    fn capabilities(&self) -> &SandboxCapabilities {
        &self.capabilities
    }

    async fn execute_code(&self, req: ExecuteCodeRequest) -> Result<ExecuteCodeResult> {
        self.send_request(
            "execute_code",
            self.client
                .post(format!("{}/execute", self.base_url))
                .json(&req),
        )
        .await
    }

    async fn health(&self) -> Result<SandboxHealthResult> {
        let mut result: SandboxHealthResult = self
            .send_request(
                "health",
                self.client.get(format!("{}/health", self.base_url)),
            )
            .await?;
        result.backend_kind = SandboxBackendKind::Remote;
        if result.sandbox_id.is_empty() {
            result.sandbox_id = self.config.sandbox_id.clone();
        }
        Ok(result)
    }
}

// ── SandboxWorkspace impl ───────────────────────────────────────

#[async_trait]
impl SandboxWorkspace for RemoteSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        // The remote API expects content as a string field with is_base64 flag,
        // matching the Python SDK contract.
        #[derive(serde::Serialize)]
        struct RemoteWriteRequest {
            path: String,
            content: String,
            mode: u32,
            is_base64: bool,
        }

        use base64::Engine;
        let remote_req = RemoteWriteRequest {
            path: req.path,
            content: base64::engine::general_purpose::STANDARD.encode(&req.content),
            mode: req.mode,
            is_base64: true,
        };

        self.send_request(
            "write_file",
            self.client
                .post(format!("{}/files", self.base_url))
                .json(&remote_req),
        )
        .await
    }

    async fn read_file(&self, path: &str) -> Result<ReadFileResult> {
        // The remote API returns content as a string with is_base64 flag.
        #[derive(serde::Deserialize)]
        struct RemoteReadResponse {
            path: String,
            content: String,
            #[serde(default)]
            is_base64: bool,
            #[serde(default)]
            size: u64,
            #[serde(default)]
            mode: u32,
            #[serde(default)]
            is_dir: bool,
            error: Option<String>,
        }

        let resp: RemoteReadResponse = self
            .send_request(
                "read_file",
                self.client
                    .get(format!("{}/files", self.base_url))
                    .query(&[("path", path)]),
            )
            .await?;

        let content = if resp.is_base64 {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(&resp.content)
                .map_err(|e| SdkError::Sandbox {
                    message: format!("invalid base64 in response: {}", e),
                    operation: "read_file".to_string(),
                    code: ErrorCode::SandboxExecutionFailed,
                })?
        } else {
            resp.content.into_bytes()
        };

        Ok(ReadFileResult {
            path: resp.path,
            content,
            size: resp.size,
            mode: resp.mode,
            is_dir: resp.is_dir,
            error: resp.error,
        })
    }

    async fn delete_file(&self, path: &str, recursive: bool) -> Result<bool> {
        #[derive(serde::Deserialize)]
        struct DeleteResponse {
            success: bool,
        }
        let resp: DeleteResponse = self
            .send_request(
                "delete_file",
                self.client
                    .delete(format!("{}/files", self.base_url))
                    .query(&[("path", path), ("recursive", &recursive.to_string())]),
            )
            .await?;
        Ok(resp.success)
    }

    async fn list_files(&self, path: &str, recursive: bool) -> Result<ListFilesResult> {
        self.send_request(
            "list_files",
            self.client
                .get(format!("{}/files/list", self.base_url))
                .query(&[("path", path), ("recursive", &recursive.to_string())]),
        )
        .await
    }
}
