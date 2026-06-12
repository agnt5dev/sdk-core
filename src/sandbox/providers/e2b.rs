//! E2B sandbox provider — REST integration with api.e2b.app.
//!
//! E2B sandboxes are created via the control-plane REST API
//! (`https://api.{domain}/sandboxes`, default domain `e2b.app`, auth
//! `X-API-Key`). A running sandbox exposes two data-plane services through
//! E2B's edge proxy at `https://{port}-{sandboxId}.{domain}`:
//!
//! - **envd** (port 49983): plain-HTTP file upload/download plus unary
//!   Connect-RPC JSON endpoints (`filesystem.Filesystem/*`).
//! - **code interpreter** (port 49999, code-interpreter templates only):
//!   `POST /execute` streaming newline-delimited JSON events.
//!
//! Code and command execution use the code interpreter, so the default
//! template is `code-interpreter-v1`. File operations work on any template.
//! Exit codes are synthesized (0 on success, 1 when an `error` event is
//! emitted) because the interpreter API does not expose process exit codes.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::providers::common::{
    provider_http_error, provider_transport_error, shell_single_quote,
};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxBackend, SandboxExecutor, SandboxProvider, SandboxWorkspace};
use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROVIDER: &str = "e2b";
const DEFAULT_DOMAIN: &str = "e2b.app";
const DEFAULT_TEMPLATE: &str = "code-interpreter-v1";
/// Default sandbox lifetime in seconds (the API default of 15s is far too
/// short for interactive use; match the official SDKs' 300s).
const DEFAULT_TIMEOUT_SECS: u64 = 300;
const ENVD_PORT: u16 = 49983;
const INTERPRETER_PORT: u16 = 49999;
/// envd user-selection header: `Basic base64("user:")`.
const ENVD_USER: &str = "user";

// ── Config ──────────────────────────────────────────────────────

/// Configuration for the E2B provider.
#[derive(Debug, Clone)]
pub struct E2bProviderConfig {
    /// E2B API key (`e2b_...`).
    pub api_key: String,
    /// Sandbox routing domain. Default: `e2b.app`.
    pub domain: String,
    /// Control-plane API URL. Default: `https://api.{domain}`.
    pub api_url: String,
    /// Template for new sandboxes. Default: `code-interpreter-v1`.
    pub template: String,
    /// HTTP request timeout for non-execution calls.
    pub timeout: Duration,
}

impl E2bProviderConfig {
    /// Build configuration from `E2B_API_KEY` (+ optional `E2B_DOMAIN`,
    /// `E2B_API_URL`, `E2B_TEMPLATE`).
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("E2B_API_KEY").map_err(|_| SdkError::Configuration {
            message: "E2B_API_KEY is required for the E2B provider".to_string(),
            field: Some("E2B_API_KEY".to_string()),
        })?;
        let domain = std::env::var("E2B_DOMAIN").unwrap_or_else(|_| DEFAULT_DOMAIN.into());
        let api_url =
            std::env::var("E2B_API_URL").unwrap_or_else(|_| format!("https://api.{}", domain));
        Ok(Self {
            api_key,
            domain,
            api_url,
            template: std::env::var("E2B_TEMPLATE").unwrap_or_else(|_| DEFAULT_TEMPLATE.into()),
            timeout: Duration::from_secs(60),
        })
    }
}

// ── Wire types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct SandboxResponse {
    #[serde(rename = "sandboxID")]
    sandbox_id: String,
    #[serde(default, rename = "envdAccessToken")]
    envd_access_token: Option<String>,
    /// Routing domain for this sandbox; may differ from the default.
    #[serde(default)]
    domain: Option<String>,
}

/// Accumulated result of a code-interpreter `/execute` ND-JSON stream.
#[derive(Debug, Default)]
struct ExecuteOutcome {
    stdout: String,
    stderr: String,
    error: Option<String>,
}

/// Parse the newline-delimited JSON stream from `POST /execute`.
///
/// Event types: `stdout`/`stderr` (`text`), `error` (`name`, `value`,
/// `traceback`), `result` and `number_of_executions` (ignored).
fn parse_execute_stream(body: &str) -> ExecuteOutcome {
    let mut outcome = ExecuteOutcome::default();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match value.get("type").and_then(|t| t.as_str()) {
            Some("stdout") => {
                if let Some(text) = value.get("text").and_then(|t| t.as_str()) {
                    outcome.stdout.push_str(text);
                }
            }
            Some("stderr") => {
                if let Some(text) = value.get("text").and_then(|t| t.as_str()) {
                    outcome.stderr.push_str(text);
                }
            }
            Some("error") => {
                let name = value
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Error");
                let detail = value.get("value").and_then(|v| v.as_str()).unwrap_or("");
                outcome.error = Some(format!("{}: {}", name, detail));
            }
            _ => {}
        }
    }
    outcome
}

/// proto3 JSON serializes 64-bit integers as strings; accept both.
fn u64_from_json(value: Option<&serde_json::Value>) -> u64 {
    match value {
        Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

// ── Provider (control plane) ────────────────────────────────────

/// Control plane for E2B sandboxes.
pub struct E2bSandboxProvider {
    config: E2bProviderConfig,
    client: reqwest::Client,
}

impl E2bSandboxProvider {
    pub fn new(config: E2bProviderConfig) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-API-Key",
            reqwest::header::HeaderValue::from_str(&config.api_key).map_err(|e| {
                SdkError::Configuration {
                    message: format!("invalid E2B API key: {}", e),
                    field: Some("api_key".to_string()),
                }
            })?,
        );
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .default_headers(headers)
            .build()
            .map_err(|e| SdkError::Configuration {
                message: format!("failed to create HTTP client: {}", e),
                field: None,
            })?;
        Ok(Self { config, client })
    }

    /// Build the provider from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::new(E2bProviderConfig::from_env()?)
    }

    fn handle_from_response(&self, resp: SandboxResponse) -> Result<E2bSandbox> {
        E2bSandbox::new(
            resp.sandbox_id,
            resp.domain.unwrap_or_else(|| self.config.domain.clone()),
            resp.envd_access_token,
            self.config.timeout,
        )
    }

    /// Create a sandbox, returning the concrete handle type.
    pub async fn create(&self, opts: CreateSandboxOptions) -> Result<E2bSandbox> {
        // Note: E2B sizes sandboxes via the template; cpu_cores/memory_mib
        // from the options are ignored.
        let mut body = serde_json::json!({
            "templateID": opts.template.as_deref().unwrap_or(&self.config.template),
            "timeout": opts.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS),
        });
        if let Some(env) = &opts.env {
            body["envVars"] = serde_json::json!(env);
        }
        if let Some(metadata) = &opts.metadata {
            body["metadata"] = serde_json::json!(metadata);
        }
        let resp: SandboxResponse = self
            .send(
                "create_sandbox",
                self.client
                    .post(format!("{}/sandboxes", self.config.api_url))
                    .json(&body),
            )
            .await?;
        self.handle_from_response(resp)
    }

    /// Connect to an existing sandbox, resuming it if paused.
    pub async fn connect(&self, sandbox_id: &str) -> Result<E2bSandbox> {
        let resp: SandboxResponse = self
            .send(
                "connect_sandbox",
                self.client
                    .post(format!(
                        "{}/sandboxes/{}/connect",
                        self.config.api_url, sandbox_id
                    ))
                    .json(&serde_json::json!({ "timeout": DEFAULT_TIMEOUT_SECS })),
            )
            .await?;
        self.handle_from_response(resp)
    }

    /// Extend the sandbox lifetime to `timeout_secs` from now.
    pub async fn set_timeout(&self, sandbox_id: &str, timeout_secs: u64) -> Result<()> {
        self.send_no_content(
            "set_timeout",
            self.client
                .post(format!(
                    "{}/sandboxes/{}/timeout",
                    self.config.api_url, sandbox_id
                ))
                .json(&serde_json::json!({ "timeout": timeout_secs })),
        )
        .await
    }

    async fn send<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<T> {
        let resp = request
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, operation, e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                operation,
                status.as_u16(),
                &body,
            ));
        }
        resp.json::<T>().await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to parse {} response: {}", PROVIDER, e),
            operation: operation.to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })
    }

    async fn send_no_content(
        &self,
        operation: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<()> {
        let resp = request
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, operation, e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                operation,
                status.as_u16(),
                &body,
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl SandboxProvider for E2bSandboxProvider {
    fn name(&self) -> &'static str {
        PROVIDER
    }

    async fn create_sandbox(&self, opts: CreateSandboxOptions) -> Result<Arc<dyn SandboxBackend>> {
        Ok(Arc::new(self.create(opts).await?))
    }

    async fn connect_sandbox(&self, sandbox_id: &str) -> Result<Arc<dyn SandboxBackend>> {
        Ok(Arc::new(self.connect(sandbox_id).await?))
    }

    async fn destroy_sandbox(&self, sandbox_id: &str) -> Result<bool> {
        self.send_no_content(
            "destroy_sandbox",
            self.client
                .delete(format!("{}/sandboxes/{}", self.config.api_url, sandbox_id)),
        )
        .await?;
        Ok(true)
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>> {
        let items: Vec<serde_json::Value> = self
            .send(
                "list_sandboxes",
                self.client
                    .get(format!("{}/sandboxes", self.config.api_url)),
            )
            .await?;
        Ok(items
            .iter()
            .map(|item| SandboxInfo {
                sandbox_id: item
                    .get("sandboxID")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                status: item
                    .get("state")
                    .and_then(|v| v.as_str())
                    .unwrap_or("running")
                    .to_string(),
                backend_kind: SandboxBackendKind::Remote,
            })
            .collect())
    }
}

// ── Sandbox handle (data plane) ─────────────────────────────────

/// A running E2B sandbox.
pub struct E2bSandbox {
    client: reqwest::Client,
    sandbox_id: String,
    domain: String,
    envd_url: String,
    interpreter_url: String,
    capabilities: SandboxCapabilities,
}

impl E2bSandbox {
    fn new(
        sandbox_id: String,
        domain: String,
        envd_access_token: Option<String>,
        timeout: Duration,
    ) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        // envd selects the OS user via Basic auth with an empty password.
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("{}:", ENVD_USER));
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Basic {}", basic))
                .expect("static basic auth header is always valid"),
        );
        if let Some(token) = &envd_access_token {
            headers.insert(
                "X-Access-Token",
                reqwest::header::HeaderValue::from_str(token).map_err(|e| {
                    SdkError::Configuration {
                        message: format!("invalid envd access token: {}", e),
                        field: None,
                    }
                })?,
            );
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(headers)
            .build()
            .map_err(|e| SdkError::Configuration {
                message: format!("failed to create HTTP client: {}", e),
                field: None,
            })?;

        let envd_url = format!("https://{}-{}.{}", ENVD_PORT, sandbox_id, domain);
        let interpreter_url = format!("https://{}-{}.{}", INTERPRETER_PORT, sandbox_id, domain);
        Ok(Self {
            client,
            sandbox_id,
            domain,
            envd_url,
            interpreter_url,
            capabilities: SandboxCapabilities {
                languages: vec![Language::Python, Language::Javascript, Language::Bash],
                supports_commands: true,
                supports_streaming: false,
                supports_git: false,
                supports_preview_url: true,
                supports_snapshots: true,
                max_execution_time_ms: 0,
                max_memory_bytes: 0,
                has_network_access: true,
            },
        })
    }

    /// Provider-native sandbox ID.
    pub fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    /// Public URL for a port inside the sandbox (no API call required).
    pub fn preview_url(&self, port: u16) -> String {
        format!("https://{}-{}.{}", port, self.sandbox_id, self.domain)
    }

    async fn execute_raw(
        &self,
        code: String,
        language: &str,
        env: Option<&std::collections::HashMap<String, String>>,
        timeout_ms: u64,
    ) -> Result<(ExecuteOutcome, u64)> {
        let started = Instant::now();
        let mut body = serde_json::json!({ "code": code, "language": language });
        if let Some(env) = env {
            body["env_vars"] = serde_json::json!(env);
        }
        let resp = self
            .client
            .post(format!("{}/execute", self.interpreter_url))
            .timeout(Duration::from_millis(timeout_ms.saturating_add(30_000)))
            .json(&body)
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "execute_code", e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "execute_code",
                status.as_u16(),
                &body,
            ));
        }
        let text = resp
            .text()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "execute_code", e))?;
        Ok((
            parse_execute_stream(&text),
            started.elapsed().as_millis() as u64,
        ))
    }

    /// Run a shell command via the code interpreter's bash kernel.
    ///
    /// Requires a code-interpreter template. The interpreter does not expose
    /// process exit codes; failures surface as an `error` event (exit code 1).
    pub async fn run_command(&self, req: RunCommandRequest) -> Result<RunCommandResult> {
        let mut line = String::new();
        if let Some(dir) = &req.working_dir {
            line.push_str(&format!("cd {} && ", shell_single_quote(dir)));
        }
        line.push_str(&req.command);
        if let Some(args) = &req.args {
            for arg in args {
                line.push(' ');
                line.push_str(&shell_single_quote(arg));
            }
        }
        let (outcome, elapsed) = self
            .execute_raw(line, "bash", req.env.as_ref(), req.timeout_ms)
            .await?;
        Ok(RunCommandResult {
            exit_code: if outcome.error.is_some() { 1 } else { 0 },
            stdout: outcome.stdout,
            stderr: outcome.stderr,
            execution_time_ms: elapsed,
            error: outcome.error,
        })
    }

    /// Unary Connect-RPC call to envd's filesystem service (JSON encoding).
    async fn filesystem_rpc(
        &self,
        method: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let resp = self
            .client
            .post(format!(
                "{}/filesystem.Filesystem/{}",
                self.envd_url, method
            ))
            .json(&body)
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, method, e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                method,
                status.as_u16(),
                &body,
            ));
        }
        resp.json().await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to parse {} response: {}", method, e),
            operation: method.to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })
    }
}

#[async_trait]
impl SandboxExecutor for E2bSandbox {
    fn backend_kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::Remote
    }

    fn capabilities(&self) -> &SandboxCapabilities {
        &self.capabilities
    }

    async fn execute_code(&self, req: ExecuteCodeRequest) -> Result<ExecuteCodeResult> {
        // The interpreter has no per-execution cwd parameter; emulate it for
        // bash, and rely on kernel defaults otherwise.
        let code = match (&req.work_dir, req.language) {
            (Some(dir), Language::Bash) => {
                format!("cd {} && {}", shell_single_quote(dir), req.code)
            }
            _ => req.code.clone(),
        };
        let (outcome, elapsed) = self
            .execute_raw(
                code,
                &req.language.to_string(),
                req.env.as_ref(),
                req.timeout_ms,
            )
            .await?;
        Ok(ExecuteCodeResult {
            exit_code: if outcome.error.is_some() { 1 } else { 0 },
            stdout: outcome.stdout,
            stderr: outcome.stderr,
            execution_time_ms: elapsed,
            error: outcome.error,
        })
    }

    async fn health(&self) -> Result<SandboxHealthResult> {
        let resp = self
            .client
            .get(format!("{}/health", self.envd_url))
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "health", e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "health",
                status.as_u16(),
                &body,
            ));
        }
        Ok(SandboxHealthResult {
            status: "running".to_string(),
            sandbox_id: self.sandbox_id.clone(),
            uptime_ms: 0,
            backend_kind: SandboxBackendKind::Remote,
            error: None,
        })
    }
}

#[async_trait]
impl SandboxWorkspace for E2bSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        let size = req.content.len() as u64;
        let resp = self
            .client
            .post(format!("{}/files", self.envd_url))
            .query(&[("path", req.path.as_str()), ("username", ENVD_USER)])
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(req.content)
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "write_file", e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "write_file",
                status.as_u16(),
                &body,
            ));
        }
        Ok(WriteFileResult {
            success: true,
            path: req.path,
            size,
            error: None,
        })
    }

    async fn read_file(&self, path: &str) -> Result<ReadFileResult> {
        let resp = self
            .client
            .get(format!("{}/files", self.envd_url))
            .query(&[("path", path), ("username", ENVD_USER)])
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "read_file", e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "read_file",
                status.as_u16(),
                &body,
            ));
        }
        let content = resp
            .bytes()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "read_file", e))?
            .to_vec();
        Ok(ReadFileResult {
            path: path.to_string(),
            size: content.len() as u64,
            content,
            mode: 0,
            is_dir: false,
            error: None,
        })
    }

    async fn delete_file(&self, path: &str, _recursive: bool) -> Result<bool> {
        // envd's Remove deletes files and directories alike; there is no
        // separate non-recursive mode.
        self.filesystem_rpc("Remove", serde_json::json!({ "path": path }))
            .await?;
        Ok(true)
    }

    async fn list_files(&self, path: &str, recursive: bool) -> Result<ListFilesResult> {
        let depth = if recursive { 64 } else { 1 };
        let resp = self
            .filesystem_rpc(
                "ListDir",
                serde_json::json!({ "path": path, "depth": depth }),
            )
            .await?;
        let entries = resp
            .get("entries")
            .and_then(|e| e.as_array())
            .cloned()
            .unwrap_or_default();
        let files: Vec<FileInfo> = entries
            .iter()
            .map(|entry| {
                let entry_path = entry
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                FileInfo {
                    name: entry
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    path: entry_path,
                    size: u64_from_json(entry.get("size")),
                    mode: entry.get("mode").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                    is_dir: entry.get("type").and_then(|v| v.as_str())
                        == Some("FILE_TYPE_DIRECTORY"),
                    mod_time: entry
                        .get("modifiedTime")
                        .and_then(|v| v.as_str())
                        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
                        .map(|t| t.timestamp_millis())
                        .unwrap_or(0),
                }
            })
            .collect();
        Ok(ListFilesResult {
            path: path.to_string(),
            total: files.len() as u64,
            files,
            error: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_execute_stream() {
        let body = concat!(
            "{\"type\":\"number_of_executions\",\"execution_count\":1}\n",
            "{\"type\":\"stdout\",\"text\":\"hello\\n\"}\n",
            "{\"type\":\"stderr\",\"text\":\"warn\\n\"}\n",
            "{\"type\":\"result\",\"is_main_result\":true,\"text\":\"42\"}\n",
        );
        let outcome = parse_execute_stream(body);
        assert_eq!(outcome.stdout, "hello\n");
        assert_eq!(outcome.stderr, "warn\n");
        assert!(outcome.error.is_none());
    }

    #[test]
    fn test_parse_execute_stream_error() {
        let body = "{\"type\":\"error\",\"name\":\"NameError\",\"value\":\"name 'x' is not defined\",\"traceback\":\"...\"}\n";
        let outcome = parse_execute_stream(body);
        assert_eq!(
            outcome.error.as_deref(),
            Some("NameError: name 'x' is not defined")
        );
    }

    #[test]
    fn test_sandbox_response_parse() {
        let json = r#"{"sandboxID":"abc123","templateID":"base","clientID":"deprecated","envdVersion":"0.2.0","envdAccessToken":"tok","domain":"e2b.app"}"#;
        let resp: SandboxResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.sandbox_id, "abc123");
        assert_eq!(resp.envd_access_token.as_deref(), Some("tok"));
        assert_eq!(resp.domain.as_deref(), Some("e2b.app"));
    }

    #[test]
    fn test_u64_from_json_number_or_string() {
        assert_eq!(u64_from_json(Some(&serde_json::json!(42))), 42);
        assert_eq!(u64_from_json(Some(&serde_json::json!("42"))), 42);
        assert_eq!(u64_from_json(Some(&serde_json::json!(null))), 0);
        assert_eq!(u64_from_json(None), 0);
    }

    #[test]
    fn test_preview_url_format() {
        let sandbox = E2bSandbox::new(
            "abc123".to_string(),
            "e2b.app".to_string(),
            None,
            Duration::from_secs(30),
        )
        .unwrap();
        assert_eq!(sandbox.preview_url(3000), "https://3000-abc123.e2b.app");
        assert_eq!(sandbox.envd_url, "https://49983-abc123.e2b.app");
        assert_eq!(sandbox.interpreter_url, "https://49999-abc123.e2b.app");
    }
}
