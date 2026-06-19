//! Together Code Interpreter (TCI) sandbox provider.
//!
//! Together's REST-able sandbox surface is the Code Interpreter API:
//! `POST https://api.together.ai/v1/tci/execute` runs Python in a session
//! that persists packages/variables for 60 minutes; `GET /v1/tci/sessions`
//! lists live sessions. Auth: `Authorization: Bearer <TOGETHER_API_KEY>`.
//!
//! Sessions are the sandbox unit: `create_sandbox` materializes one with a
//! no-op execution, `connect_sandbox` attaches to an existing session ID,
//! and sessions expire on their own (`destroy_sandbox` is unsupported).
//!
//! Only Python execution is supported; shell commands are not. File
//! operations are emulated: writes use TCI's native `files` upload
//! parameter, reads/listings run small Python snippets in the session.
//! Each operation is an execution (sessions are billed per session, not
//! per execution).
//!
//! Together Code Sandbox (the CodeSandbox-based product) requires the
//! msgpack-websocket "pitcher" protocol for command execution and is not
//! integrated here.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::providers::common::{
    parse_listing_output, provider_http_error, provider_transport_error, unsupported,
};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxBackend, SandboxExecutor, SandboxProvider, SandboxWorkspace};
use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROVIDER: &str = "together";
const DEFAULT_BASE_URL: &str = "https://api.together.ai";

// ── Config ──────────────────────────────────────────────────────

/// Configuration for the Together Code Interpreter provider.
#[derive(Debug, Clone)]
pub struct TogetherProviderConfig {
    /// Together API key.
    pub api_key: String,
    /// API base URL. Default: `https://api.together.ai`.
    pub base_url: String,
    /// HTTP request timeout for non-execution calls.
    pub timeout: Duration,
}

impl TogetherProviderConfig {
    /// Build configuration from `TOGETHER_API_KEY` (+ optional
    /// `TOGETHER_BASE_URL`).
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("TOGETHER_API_KEY").map_err(|_| SdkError::Configuration {
            message: "TOGETHER_API_KEY is required for the Together provider".to_string(),
            field: Some("TOGETHER_API_KEY".to_string()),
        })?;
        Ok(Self {
            api_key,
            base_url: std::env::var("TOGETHER_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.into()),
            timeout: Duration::from_secs(120),
        })
    }
}

// ── Wire types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct TciResponse {
    #[serde(default)]
    data: Option<TciData>,
    #[serde(default)]
    errors: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct TciData {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    outputs: Vec<TciOutput>,
}

#[derive(Debug, Deserialize)]
struct TciOutput {
    #[serde(rename = "type")]
    output_type: String,
    /// String for stdout/stderr/error; MIME map for display_data/execute_result.
    #[serde(default)]
    data: serde_json::Value,
}

/// Map TCI outputs onto stdout/stderr/error.
fn map_outputs(data: &TciData) -> ExecuteCodeResult {
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut error: Option<String> = None;
    for output in &data.outputs {
        let text = output.data.as_str().unwrap_or_default();
        match output.output_type.as_str() {
            "stdout" => stdout.push_str(text),
            "stderr" => stderr.push_str(text),
            "error" => error = Some(text.to_string()),
            // display_data / execute_result carry MIME maps; surface plain text.
            _ => {
                if let Some(text) = output.data.get("text/plain").and_then(|t| t.as_str()) {
                    stdout.push_str(text);
                }
            }
        }
    }
    let succeeded = error.is_none() && (data.status == "success" || data.status == "completed");
    ExecuteCodeResult {
        stdout,
        stderr,
        exit_code: if succeeded { 0 } else { 1 },
        execution_time_ms: 0,
        error,
    }
}

#[derive(Debug, Deserialize)]
struct SessionsResponse {
    #[serde(default)]
    data: Option<SessionsData>,
}

#[derive(Debug, Deserialize)]
struct SessionsData {
    #[serde(default)]
    sessions: Vec<SessionObject>,
}

#[derive(Debug, Deserialize)]
struct SessionObject {
    #[serde(default)]
    id: String,
}

// ── Shared HTTP helpers ─────────────────────────────────────────

struct TciClient {
    client: reqwest::Client,
    base_url: String,
}

impl TciClient {
    fn new(config: &TogetherProviderConfig) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", config.api_key)).map_err(
                |e| SdkError::Configuration {
                    message: format!("invalid Together API key: {}", e),
                    field: Some("api_key".to_string()),
                },
            )?,
        );
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .default_headers(headers)
            .build()
            .map_err(|e| SdkError::Configuration {
                message: format!("failed to create HTTP client: {}", e),
                field: None,
            })?;
        Ok(Self {
            client,
            base_url: config.base_url.clone(),
        })
    }

    /// Execute code in a TCI session, optionally uploading files first.
    async fn execute(
        &self,
        code: &str,
        session_id: Option<&str>,
        files: Option<serde_json::Value>,
        timeout_ms: u64,
    ) -> Result<TciData> {
        let mut body = serde_json::json!({ "code": code, "language": "python" });
        if let Some(session) = session_id {
            body["session_id"] = serde_json::json!(session);
        }
        if let Some(files) = files {
            body["files"] = files;
        }
        let resp = self
            .client
            .post(format!("{}/v1/tci/execute", self.base_url))
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
        let parsed: TciResponse = resp.json().await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to parse {} response: {}", PROVIDER, e),
            operation: "execute_code".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;
        if let Some(errors) = &parsed.errors {
            if !errors.is_null() {
                return Err(SdkError::Sandbox {
                    message: format!("Together TCI error: {}", errors),
                    operation: "execute_code".to_string(),
                    code: ErrorCode::SandboxExecutionFailed,
                });
            }
        }
        parsed.data.ok_or_else(|| SdkError::Sandbox {
            message: "Together TCI response missing data".to_string(),
            operation: "execute_code".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })
    }

    async fn sessions(&self) -> Result<Vec<SessionObject>> {
        let resp = self
            .client
            .get(format!("{}/v1/tci/sessions", self.base_url))
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "list_sandboxes", e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "list_sandboxes",
                status.as_u16(),
                &body,
            ));
        }
        let parsed: SessionsResponse = resp.json().await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to parse {} response: {}", PROVIDER, e),
            operation: "list_sandboxes".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;
        Ok(parsed.data.map(|d| d.sessions).unwrap_or_default())
    }
}

// ── Provider (control plane) ────────────────────────────────────

/// Control plane for Together Code Interpreter sessions.
pub struct TogetherSandboxProvider {
    tci: Arc<TciClient>,
}

impl TogetherSandboxProvider {
    pub fn new(config: TogetherProviderConfig) -> Result<Self> {
        Ok(Self {
            tci: Arc::new(TciClient::new(&config)?),
        })
    }

    /// Build the provider from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::new(TogetherProviderConfig::from_env()?)
    }

    fn handle(&self, session_id: String) -> TogetherSandbox {
        TogetherSandbox {
            tci: self.tci.clone(),
            session_id,
            capabilities: SandboxCapabilities {
                languages: vec![Language::Python],
                supports_commands: false,
                supports_streaming: false,
                supports_git: false,
                supports_preview_url: false,
                supports_snapshots: false,
                max_execution_time_ms: 0,
                max_memory_bytes: 0,
                has_network_access: true,
            },
        }
    }

    /// Create a session, returning the concrete handle type.
    pub async fn create(&self) -> Result<TogetherSandbox> {
        // Sessions are created implicitly; a no-op execution materializes one.
        let data = self.tci.execute("pass", None, None, 60_000).await?;
        if data.session_id.is_empty() {
            return Err(SdkError::Sandbox {
                message: "Together TCI did not return a session_id".to_string(),
                operation: "create_sandbox".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            });
        }
        Ok(self.handle(data.session_id))
    }
}

#[async_trait]
impl SandboxProvider for TogetherSandboxProvider {
    fn name(&self) -> &'static str {
        PROVIDER
    }

    async fn create_sandbox(&self, _opts: CreateSandboxOptions) -> Result<Arc<dyn SandboxBackend>> {
        // TCI sessions have fixed lifetime/resources; options are ignored.
        Ok(Arc::new(self.create().await?))
    }

    async fn connect_sandbox(&self, sandbox_id: &str) -> Result<Arc<dyn SandboxBackend>> {
        let sessions = self.tci.sessions().await?;
        if !sessions.iter().any(|s| s.id == sandbox_id) {
            return Err(SdkError::Sandbox {
                message: format!("Together TCI session '{}' not found or expired", sandbox_id),
                operation: "connect_sandbox".to_string(),
                code: ErrorCode::SandboxUnavailable,
            });
        }
        Ok(Arc::new(self.handle(sandbox_id.to_string())))
    }

    async fn destroy_sandbox(&self, _sandbox_id: &str) -> Result<bool> {
        // Sessions expire automatically after 60 minutes; no delete endpoint.
        Err(unsupported(PROVIDER, "destroy_sandbox"))
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>> {
        Ok(self
            .tci
            .sessions()
            .await?
            .into_iter()
            .map(|s| SandboxInfo {
                sandbox_id: s.id,
                status: "running".to_string(),
                backend_kind: SandboxBackendKind::Remote,
            })
            .collect())
    }
}

// ── Sandbox handle (data plane) ─────────────────────────────────

/// A live Together Code Interpreter session.
pub struct TogetherSandbox {
    tci: Arc<TciClient>,
    session_id: String,
    capabilities: SandboxCapabilities,
}

impl TogetherSandbox {
    /// Provider-native session ID.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Run a Python snippet in the session and return its result.
    async fn run_python(&self, code: &str, operation: &str) -> Result<ExecuteCodeResult> {
        let data = self
            .tci
            .execute(code, Some(&self.session_id), None, 60_000)
            .await?;
        let result = map_outputs(&data);
        if let Some(error) = &result.error {
            return Err(SdkError::Sandbox {
                message: error.clone(),
                operation: operation.to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            });
        }
        Ok(result)
    }
}

#[async_trait]
impl SandboxExecutor for TogetherSandbox {
    fn backend_kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::Remote
    }

    fn capabilities(&self) -> &SandboxCapabilities {
        &self.capabilities
    }

    async fn execute_code(&self, req: ExecuteCodeRequest) -> Result<ExecuteCodeResult> {
        if req.language != Language::Python {
            return Err(SdkError::Sandbox {
                message: format!(
                    "Together Code Interpreter only supports Python (requested: {})",
                    req.language
                ),
                operation: "execute_code".to_string(),
                code: ErrorCode::UnsupportedLanguage,
            });
        }
        let started = Instant::now();
        let data = self
            .tci
            .execute(&req.code, Some(&self.session_id), None, req.timeout_ms)
            .await?;
        let mut result = map_outputs(&data);
        result.execution_time_ms = started.elapsed().as_millis() as u64;
        Ok(result)
    }

    async fn health(&self) -> Result<SandboxHealthResult> {
        let sessions = self.tci.sessions().await?;
        let status = if sessions.iter().any(|s| s.id == self.session_id) {
            "running"
        } else {
            "expired"
        };
        Ok(SandboxHealthResult {
            status: status.to_string(),
            sandbox_id: self.session_id.clone(),
            uptime_ms: 0,
            backend_kind: SandboxBackendKind::Remote,
            error: None,
        })
    }
}

#[async_trait]
impl SandboxWorkspace for TogetherSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        let size = req.content.len() as u64;
        let files = serde_json::json!([{
            "name": req.path,
            "encoding": "base64",
            "content": base64::engine::general_purpose::STANDARD.encode(&req.content),
        }]);
        self.tci
            .execute("pass", Some(&self.session_id), Some(files), 60_000)
            .await?;
        Ok(WriteFileResult {
            success: true,
            path: req.path,
            size,
            error: None,
        })
    }

    async fn read_file(&self, path: &str) -> Result<ReadFileResult> {
        // JSON string literals are valid Python string literals.
        let code = format!(
            "import base64, sys\nsys.stdout.write(base64.b64encode(open({}, 'rb').read()).decode())",
            serde_json::to_string(path).expect("string serialization is infallible"),
        );
        let result = self.run_python(&code, "read_file").await?;
        let cleaned: String = result
            .stdout
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let content = base64::engine::general_purpose::STANDARD
            .decode(&cleaned)
            .map_err(|e| SdkError::Sandbox {
                message: format!("invalid base64 from session: {}", e),
                operation: "read_file".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            })?;
        Ok(ReadFileResult {
            path: path.to_string(),
            size: content.len() as u64,
            content,
            mode: 0,
            is_dir: false,
            error: None,
        })
    }

    async fn delete_file(&self, path: &str, recursive: bool) -> Result<bool> {
        let code = format!(
            r#"import os, shutil
p = {path}
if os.path.isdir(p) and not os.path.islink(p):
    shutil.rmtree(p) if {recursive} else os.rmdir(p)
else:
    os.remove(p)"#,
            path = serde_json::to_string(path).expect("string serialization is infallible"),
            recursive = if recursive { "True" } else { "False" },
        );
        self.run_python(&code, "delete_file").await?;
        Ok(true)
    }

    async fn list_files(&self, path: &str, recursive: bool) -> Result<ListFilesResult> {
        let code = format!(
            r#"import os
p = {path}
entries = []
if {recursive}:
    for dirpath, dirnames, filenames in os.walk(p):
        entries.extend(os.path.join(dirpath, n) for n in dirnames + filenames)
else:
    entries = [os.path.join(p, n) for n in os.listdir(p)]
for e in entries:
    st = os.lstat(e)
    kind = 'd' if os.path.isdir(e) and not os.path.islink(e) else 'f'
    print(f"{{kind}}|{{st.st_size}}|{{oct(st.st_mode & 0o7777)[2:]}}|{{st.st_mtime}}|{{e}}")"#,
            path = serde_json::to_string(path).expect("string serialization is infallible"),
            recursive = if recursive { "True" } else { "False" },
        );
        let result = self.run_python(&code, "list_files").await?;
        let files = parse_listing_output(&result.stdout);
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
    fn test_map_outputs_success() {
        let data: TciData = serde_json::from_str(
            r#"{"session_id":"ses_1","status":"success","outputs":[
                {"type":"stdout","data":"hello\n"},
                {"type":"stderr","data":"warn\n"},
                {"type":"execute_result","data":{"text/plain":"42"}}
            ]}"#,
        )
        .unwrap();
        let result = map_outputs(&data);
        assert_eq!(result.stdout, "hello\n42");
        assert_eq!(result.stderr, "warn\n");
        assert_eq!(result.exit_code, 0);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_map_outputs_error() {
        let data: TciData = serde_json::from_str(
            r#"{"session_id":"ses_1","status":"error","outputs":[
                {"type":"error","data":"NameError: name 'x' is not defined"}
            ]}"#,
        )
        .unwrap();
        let result = map_outputs(&data);
        assert_eq!(result.exit_code, 1);
        assert_eq!(
            result.error.as_deref(),
            Some("NameError: name 'x' is not defined")
        );
    }

    #[test]
    fn test_tci_response_with_errors() {
        let resp: TciResponse =
            serde_json::from_str(r#"{"data":null,"errors":[{"message":"invalid"}]}"#).unwrap();
        assert!(resp.data.is_none());
        assert!(!resp.errors.unwrap().is_null());
    }

    #[test]
    fn test_sessions_response_parse() {
        let resp: SessionsResponse = serde_json::from_str(
            r#"{"data":{"sessions":[{"id":"ses_abc","expires_at":"2026-06-12T01:00:00Z","execute_count":5}]},"errors":[]}"#,
        )
        .unwrap();
        let sessions = resp.data.unwrap().sessions;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "ses_abc");
    }
}
