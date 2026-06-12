//! Daytona sandbox provider — REST integration with app.daytona.io.
//!
//! Daytona sandboxes are managed via the control-plane API
//! (`https://app.daytona.io/api`) and driven via the per-sandbox toolbox
//! daemon, reached through the toolbox proxy URL returned at creation
//! (`{toolboxProxyUrl}/{sandboxId}/...`). Both planes authenticate with the
//! same `Authorization: Bearer <DAYTONA_API_KEY>` header.
//!
//! # Notes
//!
//! - `process/execute` merges stdout and stderr into a single `result`
//!   string; results therefore report combined output as `stdout` with an
//!   empty `stderr`.
//! - Python/JavaScript code runs through the native `process/code-run`
//!   endpoint; Bash runs through `process/execute`.
//! - Git operations use the toolbox's native `git/*` endpoints and are
//!   exposed as inherent methods on [`DaytonaSandbox`].
//! - Sandbox lifetime is activity-based (`autoStopInterval` minutes), not an
//!   absolute TTL; [`CreateSandboxOptions::timeout_secs`] maps to it,
//!   rounded up to whole minutes.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::providers::common::{
    interpreter_command_line, parse_listing_output, provider_http_error, provider_transport_error,
};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxBackend, SandboxExecutor, SandboxProvider, SandboxWorkspace};
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROVIDER: &str = "daytona";
const DEFAULT_API_URL: &str = "https://app.daytona.io/api";
/// States that indicate sandbox provisioning will not complete.
const FAILED_STATES: &[&str] = &["error", "build_failed", "destroyed", "destroying"];

// ── Config ──────────────────────────────────────────────────────

/// Configuration for the Daytona provider.
#[derive(Debug, Clone)]
pub struct DaytonaProviderConfig {
    /// Daytona API key (`dtn_...`).
    pub api_key: String,
    /// Control-plane base URL. Default: `https://app.daytona.io/api`.
    pub api_url: String,
    /// Target region for new sandboxes (e.g., `us`).
    pub target: Option<String>,
    /// HTTP request timeout for non-execution calls.
    pub timeout: Duration,
    /// How long to wait for a new sandbox to reach the `started` state.
    pub ready_timeout: Duration,
}

impl DaytonaProviderConfig {
    /// Build configuration from `DAYTONA_API_KEY` (+ optional
    /// `DAYTONA_API_URL`, `DAYTONA_TARGET`).
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("DAYTONA_API_KEY").map_err(|_| SdkError::Configuration {
            message: "DAYTONA_API_KEY is required for the Daytona provider".to_string(),
            field: Some("DAYTONA_API_KEY".to_string()),
        })?;
        Ok(Self {
            api_key,
            api_url: std::env::var("DAYTONA_API_URL").unwrap_or_else(|_| DEFAULT_API_URL.into()),
            target: std::env::var("DAYTONA_TARGET").ok(),
            timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(120),
        })
    }
}

// ── Wire types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SandboxObject {
    id: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    toolbox_proxy_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteResponse {
    #[serde(default)]
    exit_code: i32,
    #[serde(default)]
    result: String,
}

#[derive(Debug, Deserialize)]
struct ToolboxFileInfo {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "isDir")]
    is_dir: bool,
    #[serde(default)]
    size: u64,
    #[serde(default, rename = "modifiedAt")]
    modified_at: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    permissions: Option<String>,
}

/// Parse a file mode from the toolbox's string fields, trying the numeric
/// `permissions` form (e.g. "644") before the symbolic `mode`.
fn parse_file_mode(mode: Option<&str>, permissions: Option<&str>) -> u32 {
    for candidate in [permissions, mode].into_iter().flatten() {
        if let Ok(parsed) = u32::from_str_radix(candidate, 8) {
            return parsed;
        }
    }
    0
}

fn parse_rfc3339_ms(value: Option<&str>) -> i64 {
    value
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|t| t.timestamp_millis())
        .unwrap_or(0)
}

fn timeout_secs(timeout_ms: u64) -> u64 {
    (timeout_ms / 1000).max(1)
}

// ── Provider (control plane) ────────────────────────────────────

/// Control plane for Daytona sandboxes.
pub struct DaytonaSandboxProvider {
    config: DaytonaProviderConfig,
    client: reqwest::Client,
}

impl DaytonaSandboxProvider {
    pub fn new(config: DaytonaProviderConfig) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", config.api_key)).map_err(
                |e| SdkError::Configuration {
                    message: format!("invalid Daytona API key: {}", e),
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
        Ok(Self { config, client })
    }

    /// Build the provider from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::new(DaytonaProviderConfig::from_env()?)
    }

    async fn send<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<T> {
        send_json(PROVIDER, operation, request).await
    }

    async fn get_sandbox(&self, id: &str) -> Result<SandboxObject> {
        self.send(
            "get_sandbox",
            self.client
                .get(format!("{}/sandbox/{}", self.config.api_url, id)),
        )
        .await
    }

    async fn handle_from_object(&self, sandbox: SandboxObject) -> Result<DaytonaSandbox> {
        let toolbox_proxy_url = match &sandbox.toolbox_proxy_url {
            Some(url) => url.clone(),
            None => {
                #[derive(Deserialize)]
                struct ProxyUrl {
                    url: String,
                }
                let proxy: ProxyUrl = self
                    .send(
                        "toolbox_proxy_url",
                        self.client.get(format!(
                            "{}/sandbox/{}/toolbox-proxy-url",
                            self.config.api_url, sandbox.id
                        )),
                    )
                    .await?;
                proxy.url
            }
        };
        let toolbox_url = format!("{}/{}", toolbox_proxy_url.trim_end_matches('/'), sandbox.id);
        Ok(DaytonaSandbox {
            client: self.client.clone(),
            api_url: self.config.api_url.clone(),
            toolbox_url,
            sandbox_id: sandbox.id,
            capabilities: SandboxCapabilities {
                languages: vec![Language::Python, Language::Javascript, Language::Bash],
                supports_commands: true,
                supports_streaming: false,
                supports_git: true,
                supports_preview_url: true,
                supports_snapshots: true,
                max_execution_time_ms: 0,
                max_memory_bytes: 0,
                has_network_access: true,
            },
        })
    }

    /// Create a sandbox, returning the concrete handle type.
    pub async fn create(&self, opts: CreateSandboxOptions) -> Result<DaytonaSandbox> {
        let mut body = serde_json::Map::new();
        if let Some(snapshot) = &opts.template {
            body.insert("snapshot".into(), serde_json::json!(snapshot));
        }
        if let Some(env) = &opts.env {
            body.insert("env".into(), serde_json::json!(env));
        }
        if let Some(labels) = &opts.metadata {
            body.insert("labels".into(), serde_json::json!(labels));
        }
        if let Some(cpu) = opts.cpu_cores {
            body.insert("cpu".into(), serde_json::json!(cpu));
        }
        if let Some(mib) = opts.memory_mib {
            body.insert("memory".into(), serde_json::json!(mib.div_ceil(1024)));
        }
        if let Some(secs) = opts.timeout_secs {
            body.insert(
                "autoStopInterval".into(),
                serde_json::json!(secs.div_ceil(60)),
            );
        }
        if let Some(target) = &self.config.target {
            body.insert("target".into(), serde_json::json!(target));
        }

        let mut sandbox: SandboxObject = self
            .send(
                "create_sandbox",
                self.client
                    .post(format!("{}/sandbox", self.config.api_url))
                    .json(&serde_json::Value::Object(body)),
            )
            .await?;

        // Wait for the sandbox to leave the provisioning states.
        let deadline = Instant::now() + self.config.ready_timeout;
        loop {
            let state = sandbox.state.as_deref().unwrap_or("");
            if state == "started" {
                break;
            }
            if FAILED_STATES.contains(&state) {
                return Err(SdkError::Sandbox {
                    message: format!("Daytona sandbox {} entered state '{}'", sandbox.id, state),
                    operation: "create_sandbox".to_string(),
                    code: ErrorCode::SandboxUnavailable,
                });
            }
            if Instant::now() >= deadline {
                return Err(SdkError::Sandbox {
                    message: format!(
                        "Daytona sandbox {} not started within {:?} (state: '{}')",
                        sandbox.id, self.config.ready_timeout, state
                    ),
                    operation: "create_sandbox".to_string(),
                    code: ErrorCode::ExecutionTimeout,
                });
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            sandbox = self.get_sandbox(&sandbox.id).await?;
        }

        self.handle_from_object(sandbox).await
    }

    /// Connect to an existing sandbox by ID or name.
    pub async fn connect(&self, sandbox_id: &str) -> Result<DaytonaSandbox> {
        let sandbox = self.get_sandbox(sandbox_id).await?;
        self.handle_from_object(sandbox).await
    }
}

#[async_trait]
impl SandboxProvider for DaytonaSandboxProvider {
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
        let _: serde_json::Value = self
            .send(
                "destroy_sandbox",
                self.client
                    .delete(format!("{}/sandbox/{}", self.config.api_url, sandbox_id)),
            )
            .await?;
        Ok(true)
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>> {
        #[derive(Deserialize)]
        struct ListResponse {
            #[serde(default)]
            items: Vec<SandboxObject>,
        }
        let resp: ListResponse = self
            .send(
                "list_sandboxes",
                self.client.get(format!("{}/sandbox", self.config.api_url)),
            )
            .await?;
        Ok(resp
            .items
            .into_iter()
            .map(|s| SandboxInfo {
                sandbox_id: s.id,
                status: s.state.unwrap_or_else(|| "unknown".to_string()),
                backend_kind: SandboxBackendKind::Remote,
            })
            .collect())
    }
}

// ── Sandbox handle (data plane) ─────────────────────────────────

/// A running Daytona sandbox, driven via its toolbox daemon.
pub struct DaytonaSandbox {
    client: reqwest::Client,
    api_url: String,
    toolbox_url: String,
    sandbox_id: String,
    capabilities: SandboxCapabilities,
}

/// Preview URL details for a sandbox port.
#[derive(Debug, Clone, Deserialize)]
pub struct DaytonaPreviewUrl {
    /// Public URL for the port.
    pub url: String,
    /// Token for the `x-daytona-preview-token` header (private sandboxes).
    #[serde(default)]
    pub token: Option<String>,
}

impl DaytonaSandbox {
    /// Provider-native sandbox ID.
    pub fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    async fn send<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<T> {
        send_json(PROVIDER, operation, request).await
    }

    /// Run a shell command via the toolbox `process/execute` endpoint.
    ///
    /// stdout and stderr are merged by the Daytona daemon; the combined
    /// output is reported as `stdout`.
    pub async fn run_command(&self, req: RunCommandRequest) -> Result<RunCommandResult> {
        let started = Instant::now();
        let mut command = req.command.clone();
        if let Some(args) = &req.args {
            for arg in args {
                command.push(' ');
                command.push_str(&crate::sandbox::providers::common::shell_single_quote(arg));
            }
        }
        let body = serde_json::json!({
            "command": command,
            "cwd": req.working_dir,
            "envs": req.env,
            "timeout": timeout_secs(req.timeout_ms),
        });
        let resp: ExecuteResponse = self
            .send(
                "run_command",
                self.client
                    .post(format!("{}/process/execute", self.toolbox_url))
                    .timeout(Duration::from_millis(req.timeout_ms.saturating_add(30_000)))
                    .json(&body),
            )
            .await?;
        Ok(RunCommandResult {
            stdout: resp.result,
            stderr: String::new(),
            exit_code: resp.exit_code,
            execution_time_ms: started.elapsed().as_millis() as u64,
            error: None,
        })
    }

    /// Public preview URL for a port exposed by the sandbox.
    pub async fn preview_url(&self, port: u16) -> Result<DaytonaPreviewUrl> {
        self.send(
            "get_preview_url",
            self.client.get(format!(
                "{}/sandbox/{}/ports/{}/preview-url",
                self.api_url, self.sandbox_id, port
            )),
        )
        .await
    }

    // ── Git operations (native toolbox endpoints) ───────────────

    /// Clone a git repository inside the sandbox.
    pub async fn git_clone(&self, req: GitCloneRequest) -> Result<GitCloneResult> {
        let path = req.target_dir.clone().unwrap_or_else(|| {
            req.url
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("repo")
                .trim_end_matches(".git")
                .to_string()
        });
        let body = serde_json::json!({
            "url": req.url,
            "path": path,
            "branch": req.branch,
            "username": req.username,
            "password": req.password,
        });
        let _: serde_json::Value = self
            .send(
                "git_clone",
                self.client
                    .post(format!("{}/git/clone", self.toolbox_url))
                    .json(&body),
            )
            .await
            .or_else(empty_body_ok)?;
        Ok(GitCloneResult {
            success: true,
            path,
            branch: req.branch.unwrap_or_default(),
            commit_sha: String::new(),
            error: None,
        })
    }

    /// Git status of a repository in the sandbox.
    pub async fn git_status(&self, path: Option<&str>) -> Result<GitStatusResult> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct StatusResponse {
            #[serde(default)]
            current_branch: String,
            #[serde(default)]
            file_status: Vec<FileStatus>,
            #[serde(default)]
            ahead: i32,
            #[serde(default)]
            behind: i32,
        }
        #[derive(Deserialize)]
        struct FileStatus {
            #[serde(default)]
            name: String,
            #[serde(default)]
            staging: String,
            #[serde(default)]
            worktree: String,
        }

        let resp: StatusResponse = self
            .send(
                "git_status",
                self.client
                    .get(format!("{}/git/status", self.toolbox_url))
                    .query(&[("path", path.unwrap_or("."))]),
            )
            .await?;

        let files: Vec<GitStatusFile> = resp
            .file_status
            .into_iter()
            .map(|f| {
                let staged = !f.staging.is_empty() && !f.staging.eq_ignore_ascii_case("unmodified");
                let status = if staged { f.staging } else { f.worktree };
                GitStatusFile {
                    path: f.name,
                    status: status.to_lowercase(),
                    staged,
                }
            })
            .collect();
        Ok(GitStatusResult {
            branch: resp.current_branch,
            commit_sha: String::new(),
            is_clean: files.is_empty(),
            ahead: resp.ahead,
            behind: resp.behind,
            files,
            error: None,
        })
    }

    /// Stage files and create a commit.
    pub async fn git_commit(&self, req: GitCommitRequest) -> Result<GitCommitResult> {
        let path = req.path.clone().unwrap_or_else(|| ".".to_string());

        // Stage requested files ("." when committing all changes).
        let files = if req.all || req.files.is_none() {
            vec![".".to_string()]
        } else {
            req.files.clone().unwrap_or_default()
        };
        let _: serde_json::Value = self
            .send(
                "git_add",
                self.client
                    .post(format!("{}/git/add", self.toolbox_url))
                    .json(&serde_json::json!({ "path": path, "files": files })),
            )
            .await
            .or_else(empty_body_ok)?;

        // The toolbox requires separate author name and email fields.
        let (author, email) = split_author(req.author.as_deref());
        #[derive(Deserialize)]
        struct CommitResponse {
            #[serde(default)]
            hash: String,
        }
        let resp: CommitResponse = self
            .send(
                "git_commit",
                self.client
                    .post(format!("{}/git/commit", self.toolbox_url))
                    .json(&serde_json::json!({
                        "path": path,
                        "message": req.message,
                        "author": author,
                        "email": email,
                    })),
            )
            .await?;
        Ok(GitCommitResult {
            success: true,
            commit_sha: resp.hash,
            error: None,
        })
    }

    /// Push commits to the remote.
    pub async fn git_push(&self, req: GitPushRequest) -> Result<GitPushResult> {
        let _: serde_json::Value = self
            .send(
                "git_push",
                self.client
                    .post(format!("{}/git/push", self.toolbox_url))
                    .json(&serde_json::json!({
                        "path": req.path.clone().unwrap_or_else(|| ".".to_string()),
                        "username": req.username,
                        "password": req.password,
                    })),
            )
            .await
            .or_else(empty_body_ok)?;
        Ok(GitPushResult {
            success: true,
            error: None,
        })
    }
}

/// Treat a JSON-parse failure of an empty 2xx body as success.
fn empty_body_ok(e: SdkError) -> Result<serde_json::Value> {
    match e {
        SdkError::Sandbox { ref message, .. } if message.contains("parse") => {
            Ok(serde_json::Value::Null)
        }
        other => Err(other),
    }
}

/// Split an `"Author Name <email>"` string into name and email parts.
fn split_author(author: Option<&str>) -> (String, String) {
    let author = author.unwrap_or("AGNT5 <agnt5@agnt5.dev>");
    if let Some((name, rest)) = author.split_once('<') {
        let email = rest.trim_end_matches('>').trim().to_string();
        (name.trim().to_string(), email)
    } else {
        (author.trim().to_string(), "agnt5@agnt5.dev".to_string())
    }
}

/// Shared JSON request/response helper.
async fn send_json<T: serde::de::DeserializeOwned>(
    provider: &str,
    operation: &str,
    request: reqwest::RequestBuilder,
) -> Result<T> {
    let resp = request
        .send()
        .await
        .map_err(|e| provider_transport_error(provider, operation, e))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(provider_http_error(
            provider,
            operation,
            status.as_u16(),
            &body,
        ));
    }
    let body = resp
        .bytes()
        .await
        .map_err(|e| provider_transport_error(provider, operation, e))?;
    serde_json::from_slice(&body).map_err(|e| SdkError::Sandbox {
        message: format!("failed to parse {} response: {}", provider, e),
        operation: operation.to_string(),
        code: ErrorCode::SandboxExecutionFailed,
    })
}

#[async_trait]
impl SandboxExecutor for DaytonaSandbox {
    fn backend_kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::Remote
    }

    fn capabilities(&self) -> &SandboxCapabilities {
        &self.capabilities
    }

    async fn execute_code(&self, req: ExecuteCodeRequest) -> Result<ExecuteCodeResult> {
        let started = Instant::now();
        match req.language {
            // Bash has no native code-run language; go through the shell.
            Language::Bash => {
                let command = interpreter_command_line(&req);
                let result = self
                    .run_command(RunCommandRequest {
                        command,
                        args: None,
                        working_dir: req.work_dir,
                        env: req.env,
                        timeout_ms: req.timeout_ms,
                    })
                    .await?;
                Ok(ExecuteCodeResult {
                    stdout: result.stdout,
                    stderr: result.stderr,
                    exit_code: result.exit_code,
                    execution_time_ms: result.execution_time_ms,
                    error: result.error,
                })
            }
            Language::Python | Language::Javascript => {
                let body = serde_json::json!({
                    "code": req.code,
                    "language": req.language.to_string(),
                    "envs": req.env,
                    "timeout": timeout_secs(req.timeout_ms),
                });
                let resp: ExecuteResponse = self
                    .send(
                        "execute_code",
                        self.client
                            .post(format!("{}/process/code-run", self.toolbox_url))
                            .timeout(Duration::from_millis(req.timeout_ms.saturating_add(30_000)))
                            .json(&body),
                    )
                    .await?;
                Ok(ExecuteCodeResult {
                    stdout: resp.result,
                    stderr: String::new(),
                    exit_code: resp.exit_code,
                    execution_time_ms: started.elapsed().as_millis() as u64,
                    error: None,
                })
            }
        }
    }

    async fn health(&self) -> Result<SandboxHealthResult> {
        let sandbox: SandboxObject = self
            .send(
                "health",
                self.client
                    .get(format!("{}/sandbox/{}", self.api_url, self.sandbox_id)),
            )
            .await?;
        Ok(SandboxHealthResult {
            status: sandbox.state.unwrap_or_else(|| "unknown".to_string()),
            sandbox_id: self.sandbox_id.clone(),
            uptime_ms: 0,
            backend_kind: SandboxBackendKind::Remote,
            error: None,
        })
    }
}

#[async_trait]
impl SandboxWorkspace for DaytonaSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        let size = req.content.len() as u64;
        let part = reqwest::multipart::Part::bytes(req.content)
            .file_name(req.path.rsplit('/').next().unwrap_or("file").to_string())
            .mime_str("application/octet-stream")
            .map_err(|e| SdkError::Sandbox {
                message: format!("failed to build multipart body: {}", e),
                operation: "write_file".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            })?;
        let form = reqwest::multipart::Form::new().part("file", part);

        let resp = self
            .client
            .post(format!("{}/files/upload", self.toolbox_url))
            .query(&[("path", req.path.as_str())])
            .multipart(form)
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
            .get(format!("{}/files/download", self.toolbox_url))
            .query(&[("path", path)])
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

    async fn delete_file(&self, path: &str, recursive: bool) -> Result<bool> {
        let resp = self
            .client
            .delete(format!("{}/files", self.toolbox_url))
            .query(&[("path", path), ("recursive", &recursive.to_string())])
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "delete_file", e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "delete_file",
                status.as_u16(),
                &body,
            ));
        }
        Ok(true)
    }

    async fn list_files(&self, path: &str, recursive: bool) -> Result<ListFilesResult> {
        if recursive {
            // The toolbox files API lists a single directory; recursive
            // listings go through `find` (Daytona images are GNU userlands).
            let result = self
                .run_command(RunCommandRequest {
                    command: format!(
                        "find {} -mindepth 1 -printf '%y|%s|%m|%T@|%p\\n'",
                        crate::sandbox::providers::common::shell_single_quote(path)
                    ),
                    args: None,
                    working_dir: None,
                    env: None,
                    timeout_ms: 30_000,
                })
                .await?;
            if result.exit_code != 0 {
                return Err(SdkError::Sandbox {
                    message: format!("list_files failed: {}", result.stdout),
                    operation: "list_files".to_string(),
                    code: ErrorCode::SandboxExecutionFailed,
                });
            }
            let files = parse_listing_output(&result.stdout);
            return Ok(ListFilesResult {
                path: path.to_string(),
                total: files.len() as u64,
                files,
                error: None,
            });
        }

        let entries: Vec<ToolboxFileInfo> = self
            .send(
                "list_files",
                self.client
                    .get(format!("{}/files", self.toolbox_url))
                    .query(&[("path", path)]),
            )
            .await?;
        let files: Vec<FileInfo> = entries
            .into_iter()
            .map(|e| FileInfo {
                path: format!("{}/{}", path.trim_end_matches('/'), e.name),
                mode: parse_file_mode(e.mode.as_deref(), e.permissions.as_deref()),
                mod_time: parse_rfc3339_ms(e.modified_at.as_deref()),
                name: e.name,
                size: e.size,
                is_dir: e.is_dir,
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
    fn test_parse_file_mode() {
        assert_eq!(parse_file_mode(None, Some("644")), 0o644);
        assert_eq!(parse_file_mode(Some("755"), None), 0o755);
        assert_eq!(parse_file_mode(Some("drwxr-xr-x"), Some("rw-")), 0);
        assert_eq!(parse_file_mode(None, None), 0);
    }

    #[test]
    fn test_parse_rfc3339_ms() {
        assert_eq!(
            parse_rfc3339_ms(Some("2026-06-12T00:00:00Z")),
            1781222400000
        );
        assert_eq!(parse_rfc3339_ms(Some("not a date")), 0);
        assert_eq!(parse_rfc3339_ms(None), 0);
    }

    #[test]
    fn test_split_author() {
        assert_eq!(
            split_author(Some("Jane Doe <jane@example.com>")),
            ("Jane Doe".to_string(), "jane@example.com".to_string())
        );
        let (name, email) = split_author(None);
        assert_eq!(name, "AGNT5");
        assert_eq!(email, "agnt5@agnt5.dev");
    }

    #[test]
    fn test_timeout_secs() {
        assert_eq!(timeout_secs(30_000), 30);
        assert_eq!(timeout_secs(500), 1);
        assert_eq!(timeout_secs(0), 1);
    }

    #[test]
    fn test_execute_response_parse() {
        let json = r#"{"exitCode": 0, "result": "hello\n"}"#;
        let resp: ExecuteResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.exit_code, 0);
        assert_eq!(resp.result, "hello\n");
    }

    #[test]
    fn test_sandbox_object_parse() {
        let json = r#"{"id":"sb-1","state":"started","toolboxProxyUrl":"https://proxy.app.daytona.io/toolbox","organizationId":"org"}"#;
        let sandbox: SandboxObject = serde_json::from_str(json).unwrap();
        assert_eq!(sandbox.id, "sb-1");
        assert_eq!(sandbox.state.as_deref(), Some("started"));
        assert_eq!(
            sandbox.toolbox_proxy_url.as_deref(),
            Some("https://proxy.app.daytona.io/toolbox")
        );
    }
}
