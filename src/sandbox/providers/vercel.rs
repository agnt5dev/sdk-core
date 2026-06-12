//! Vercel Sandbox provider — REST integration with api.vercel.com.
//!
//! Vercel Sandboxes are Firecracker microVMs created via the documented
//! `/v2/sandboxes` REST API. Command execution streams ND-JSON; file writes
//! upload a gzipped tar archive; file reads return raw bytes.
//!
//! # Authentication
//!
//! Two modes, matching the official `@vercel/sandbox` SDK:
//!
//! - **OIDC token** (`VERCEL_OIDC_TOKEN`): used directly as the Bearer token;
//!   team and project are encoded in the token.
//! - **Access token** (`VERCEL_TOKEN` + `VERCEL_TEAM_ID` + `VERCEL_PROJECT_ID`):
//!   team is passed as the `teamId` query parameter, project in request bodies.
//!
//! # Capabilities
//!
//! Runtimes are `node22`/`node24`/`node26`/`python3.13` (set via
//! [`CreateSandboxOptions::template`], default `node24`). All runtimes include
//! bash; JavaScript vs Python availability depends on the chosen runtime.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::providers::common::{
    interpreter_argv, parse_listing_output, provider_http_error, provider_transport_error,
};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxBackend, SandboxExecutor, SandboxProvider, SandboxWorkspace};
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROVIDER: &str = "vercel";
const DEFAULT_BASE_URL: &str = "https://api.vercel.com";
const DEFAULT_RUNTIME: &str = "node24";
/// Default sandbox lifetime (5 minutes, matching Vercel's default).
const DEFAULT_TIMEOUT_SECS: u64 = 300;
/// Default working directory inside a Vercel sandbox.
const DEFAULT_WORKDIR: &str = "/vercel/sandbox";

// ── Config ──────────────────────────────────────────────────────

/// Configuration for the Vercel Sandbox provider.
#[derive(Debug, Clone)]
pub struct VercelProviderConfig {
    /// Bearer token: an OIDC token or a Vercel access token.
    pub token: String,
    /// Team ID (required with access tokens, embedded in OIDC tokens).
    pub team_id: Option<String>,
    /// Project ID (required with access tokens, embedded in OIDC tokens).
    pub project_id: Option<String>,
    /// API base URL. Default: `https://api.vercel.com`.
    pub base_url: String,
    /// HTTP request timeout for non-execution calls.
    pub timeout: Duration,
}

impl VercelProviderConfig {
    /// Build configuration from environment variables.
    ///
    /// Prefers `VERCEL_OIDC_TOKEN`; falls back to `VERCEL_TOKEN` +
    /// `VERCEL_TEAM_ID` + `VERCEL_PROJECT_ID`.
    pub fn from_env() -> Result<Self> {
        let base_url =
            std::env::var("VERCEL_SANDBOX_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.into());

        if let Ok(oidc) = std::env::var("VERCEL_OIDC_TOKEN") {
            return Ok(Self {
                token: oidc,
                team_id: std::env::var("VERCEL_TEAM_ID").ok(),
                project_id: std::env::var("VERCEL_PROJECT_ID").ok(),
                base_url,
                timeout: Duration::from_secs(60),
            });
        }

        let token = std::env::var("VERCEL_TOKEN").map_err(|_| SdkError::Configuration {
            message: "VERCEL_OIDC_TOKEN or VERCEL_TOKEN is required for the Vercel provider"
                .to_string(),
            field: Some("VERCEL_TOKEN".to_string()),
        })?;
        let team_id = std::env::var("VERCEL_TEAM_ID").map_err(|_| SdkError::Configuration {
            message: "VERCEL_TEAM_ID is required when using VERCEL_TOKEN".to_string(),
            field: Some("VERCEL_TEAM_ID".to_string()),
        })?;
        let project_id =
            std::env::var("VERCEL_PROJECT_ID").map_err(|_| SdkError::Configuration {
                message: "VERCEL_PROJECT_ID is required when using VERCEL_TOKEN".to_string(),
                field: Some("VERCEL_PROJECT_ID".to_string()),
            })?;

        Ok(Self {
            token,
            team_id: Some(team_id),
            project_id: Some(project_id),
            base_url,
            timeout: Duration::from_secs(60),
        })
    }
}

// ── Wire types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct SandboxEnvelope {
    sandbox: SandboxObject,
    session: SessionObject,
    #[serde(default)]
    routes: Vec<RouteObject>,
}

#[derive(Debug, Deserialize)]
struct SandboxObject {
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SessionObject {
    id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RouteObject {
    url: String,
    port: u16,
}

/// Accumulated result of an ND-JSON command stream.
#[derive(Debug, Default)]
struct CommandOutcome {
    stdout: String,
    stderr: String,
    exit_code: Option<i32>,
    error: Option<String>,
}

/// Parse the ND-JSON stream returned by `POST .../cmd?wait=true&logs=true`.
///
/// Lines are either command objects (`{"command": {..., "exitCode": ...}}`)
/// or log lines (`{"stream": "stdout"|"stderr"|"error", "data": ...}`); a
/// line may carry both.
fn parse_command_stream(body: &str) -> CommandOutcome {
    let mut outcome = CommandOutcome::default();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if let Some(stream) = value.get("stream").and_then(|s| s.as_str()) {
            match stream {
                "stdout" => {
                    if let Some(data) = value.get("data").and_then(|d| d.as_str()) {
                        outcome.stdout.push_str(data);
                    }
                }
                "stderr" => {
                    if let Some(data) = value.get("data").and_then(|d| d.as_str()) {
                        outcome.stderr.push_str(data);
                    }
                }
                "error" => {
                    let message = value
                        .get("data")
                        .map(|d| {
                            d.get("message")
                                .and_then(|m| m.as_str())
                                .map(str::to_string)
                                .unwrap_or_else(|| d.to_string())
                        })
                        .unwrap_or_else(|| "unknown stream error".to_string());
                    outcome.error = Some(message);
                }
                _ => {}
            }
        }
        if let Some(command) = value.get("command") {
            if let Some(code) = command.get("exitCode").and_then(|c| c.as_i64()) {
                outcome.exit_code = Some(code as i32);
            }
        }
    }
    outcome
}

// ── Provider (control plane) ────────────────────────────────────

/// Control plane for Vercel Sandboxes.
pub struct VercelSandboxProvider {
    config: VercelProviderConfig,
    client: reqwest::Client,
}

impl VercelSandboxProvider {
    pub fn new(config: VercelProviderConfig) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", config.token)).map_err(
                |e| SdkError::Configuration {
                    message: format!("invalid Vercel token: {}", e),
                    field: Some("token".to_string()),
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
        Self::new(VercelProviderConfig::from_env()?)
    }

    fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        if let Some(team) = &self.config.team_id {
            q.push(("teamId".to_string(), team.clone()));
        }
        if let Some(project) = &self.config.project_id {
            q.push(("projectId".to_string(), project.clone()));
        }
        q
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

    fn handle_from_envelope(&self, envelope: SandboxEnvelope) -> VercelSandbox {
        VercelSandbox {
            client: self.client.clone(),
            base_url: self.config.base_url.clone(),
            query: self.query(),
            name: envelope.sandbox.name.unwrap_or_default(),
            session_id: envelope.session.id,
            routes: envelope.routes,
            capabilities: SandboxCapabilities {
                languages: vec![Language::Python, Language::Javascript, Language::Bash],
                supports_commands: true,
                supports_streaming: true,
                supports_git: false,
                supports_preview_url: true,
                supports_snapshots: true,
                max_execution_time_ms: 0,
                max_memory_bytes: 0,
                has_network_access: true,
            },
        }
    }

    /// Create a sandbox, returning the concrete handle type.
    pub async fn create(&self, opts: CreateSandboxOptions) -> Result<VercelSandbox> {
        let name = format!("agnt5-{}", uuid::Uuid::new_v4().simple());
        let mut body = serde_json::json!({
            "name": name,
            "runtime": opts.template.as_deref().unwrap_or(DEFAULT_RUNTIME),
            "timeout": opts.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS) * 1000,
        });
        if let Some(cpu) = opts.cpu_cores {
            body["resources"] = serde_json::json!({ "vcpus": cpu });
        }
        if let Some(env) = &opts.env {
            body["env"] = serde_json::json!(env);
        }
        if let Some(project) = &self.config.project_id {
            body["projectId"] = serde_json::json!(project);
        }

        let envelope: SandboxEnvelope = self
            .send(
                "create_sandbox",
                self.client
                    .post(format!("{}/v2/sandboxes", self.config.base_url))
                    .query(&self.query())
                    .json(&body),
            )
            .await?;
        Ok(self.handle_from_envelope(envelope))
    }

    /// Connect to an existing sandbox by name, resuming it if stopped.
    pub async fn connect(&self, name: &str) -> Result<VercelSandbox> {
        let mut query = self.query();
        query.push(("resume".to_string(), "true".to_string()));
        let envelope: SandboxEnvelope = self
            .send(
                "connect_sandbox",
                self.client
                    .get(format!("{}/v2/sandboxes/{}", self.config.base_url, name))
                    .query(&query),
            )
            .await?;
        Ok(self.handle_from_envelope(envelope))
    }
}

#[async_trait]
impl SandboxProvider for VercelSandboxProvider {
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
        let resp = self
            .client
            .delete(format!(
                "{}/v2/sandboxes/{}",
                self.config.base_url, sandbox_id
            ))
            .query(&self.query())
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "destroy_sandbox", e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "destroy_sandbox",
                status.as_u16(),
                &body,
            ));
        }
        Ok(true)
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>> {
        let value: serde_json::Value = self
            .send(
                "list_sandboxes",
                self.client
                    .get(format!("{}/v2/sandboxes", self.config.base_url))
                    .query(&self.query()),
            )
            .await?;
        let items = value
            .get("sandboxes")
            .and_then(|s| s.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(items
            .iter()
            .map(|item| SandboxInfo {
                sandbox_id: item
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string(),
                status: item
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                backend_kind: SandboxBackendKind::Remote,
            })
            .collect())
    }
}

// ── Sandbox handle (data plane) ─────────────────────────────────

/// A running Vercel Sandbox session.
pub struct VercelSandbox {
    client: reqwest::Client,
    base_url: String,
    query: Vec<(String, String)>,
    name: String,
    session_id: String,
    routes: Vec<RouteObject>,
    capabilities: SandboxCapabilities,
}

impl VercelSandbox {
    /// Sandbox name (the provider-native identifier).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Session ID for the current VM run of this sandbox.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Public URL for a port declared at sandbox creation.
    pub fn preview_url(&self, port: u16) -> Option<&str> {
        self.routes
            .iter()
            .find(|r| r.port == port)
            .map(|r| r.url.as_str())
    }

    fn session_url(&self, suffix: &str) -> String {
        format!(
            "{}/v2/sandboxes/sessions/{}{}",
            self.base_url, self.session_id, suffix
        )
    }

    /// Run a shell command in the sandbox and wait for completion.
    ///
    /// Inherent method (not part of the universal trait), mirroring
    /// [`RemoteSandbox::run_command`](crate::sandbox::RemoteSandbox::run_command).
    pub async fn run_command(&self, req: RunCommandRequest) -> Result<RunCommandResult> {
        let started = Instant::now();
        let body = serde_json::json!({
            "command": req.command,
            "args": req.args.clone().unwrap_or_default(),
            "cwd": req.working_dir,
            "env": req.env.clone().unwrap_or_default(),
            "wait": true,
            "logs": true,
            "timeout": req.timeout_ms,
        });
        let resp = self
            .client
            .post(self.session_url("/cmd"))
            .query(&self.query)
            // Command runtime is bounded by the API-side `timeout`; give the
            // HTTP layer headroom beyond it.
            .timeout(Duration::from_millis(req.timeout_ms.saturating_add(30_000)))
            .json(&body)
            .send()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "run_command", e))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(provider_http_error(
                PROVIDER,
                "run_command",
                status.as_u16(),
                &body,
            ));
        }

        let text = resp
            .text()
            .await
            .map_err(|e| provider_transport_error(PROVIDER, "run_command", e))?;
        let outcome = parse_command_stream(&text);
        Ok(RunCommandResult {
            stdout: outcome.stdout,
            stderr: outcome.stderr,
            exit_code: outcome.exit_code.unwrap_or(-1),
            execution_time_ms: started.elapsed().as_millis() as u64,
            error: outcome.error,
        })
    }
}

#[async_trait]
impl SandboxExecutor for VercelSandbox {
    fn backend_kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::Remote
    }

    fn capabilities(&self) -> &SandboxCapabilities {
        &self.capabilities
    }

    async fn execute_code(&self, req: ExecuteCodeRequest) -> Result<ExecuteCodeResult> {
        let (program, args) = interpreter_argv(&req);
        let result = self
            .run_command(RunCommandRequest {
                command: program,
                args: Some(args),
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

    async fn health(&self) -> Result<SandboxHealthResult> {
        let value: serde_json::Value = {
            let resp = self
                .client
                .get(self.session_url(""))
                .query(&self.query)
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
            resp.json()
                .await
                .map_err(|e| provider_transport_error(PROVIDER, "health", e))?
        };
        // Session status may be top-level or nested under "session".
        let status = value
            .get("status")
            .or_else(|| value.get("session").and_then(|s| s.get("status")))
            .and_then(|s| s.as_str())
            .unwrap_or("unknown")
            .to_string();
        Ok(SandboxHealthResult {
            status,
            sandbox_id: self.name.clone(),
            uptime_ms: 0,
            backend_kind: SandboxBackendKind::Remote,
            error: None,
        })
    }
}

#[async_trait]
impl SandboxWorkspace for VercelSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        // The fs/write API takes a gzipped tar archive; entry paths are
        // resolved relative to the x-cwd header.
        let (cwd, entry_path) = match req.path.strip_prefix('/') {
            Some(rest) => ("/", rest.to_string()),
            None => (DEFAULT_WORKDIR, req.path.clone()),
        };

        let archive = build_tar_gz(&entry_path, &req.content, req.mode)?;
        let size = req.content.len() as u64;

        let resp = self
            .client
            .post(self.session_url("/fs/write"))
            .query(&self.query)
            .header(reqwest::header::CONTENT_TYPE, "application/gzip")
            .header("x-cwd", cwd)
            .body(archive)
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
            .post(self.session_url("/fs/read"))
            .query(&self.query)
            .json(&serde_json::json!({ "path": path }))
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
        let mut args = vec!["-f".to_string()];
        if recursive {
            args.push("-r".to_string());
        }
        args.push("--".to_string());
        args.push(path.to_string());
        let result = self
            .run_command(RunCommandRequest {
                command: "rm".to_string(),
                args: Some(args),
                working_dir: None,
                env: None,
                timeout_ms: 30_000,
            })
            .await?;
        Ok(result.exit_code == 0)
    }

    async fn list_files(&self, path: &str, recursive: bool) -> Result<ListFilesResult> {
        // Amazon Linux 2023 ships GNU findutils, so -printf is available.
        let mut args = vec![path.to_string(), "-mindepth".to_string(), "1".to_string()];
        if !recursive {
            args.push("-maxdepth".to_string());
            args.push("1".to_string());
        }
        args.push("-printf".to_string());
        args.push("%y|%s|%m|%T@|%p\\n".to_string());

        let result = self
            .run_command(RunCommandRequest {
                command: "find".to_string(),
                args: Some(args),
                working_dir: None,
                env: None,
                timeout_ms: 30_000,
            })
            .await?;
        if result.exit_code != 0 {
            return Err(SdkError::Sandbox {
                message: format!("list_files failed: {}", result.stderr),
                operation: "list_files".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            });
        }
        let files = parse_listing_output(&result.stdout);
        Ok(ListFilesResult {
            path: path.to_string(),
            total: files.len() as u64,
            files,
            error: None,
        })
    }
}

/// Build a gzipped tar archive containing a single file entry.
fn build_tar_gz(entry_path: &str, content: &[u8], mode: u32) -> Result<Vec<u8>> {
    let tar_error = |e: std::io::Error| SdkError::Sandbox {
        message: format!("failed to build tar archive: {}", e),
        operation: "write_file".to_string(),
        code: ErrorCode::SandboxExecutionFailed,
    };

    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(content.len() as u64);
    header.set_mode(mode);
    header.set_cksum();
    builder
        .append_data(&mut header, entry_path, content)
        .map_err(tar_error)?;
    let tar_bytes = builder.into_inner().map_err(tar_error)?;

    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&tar_bytes).map_err(tar_error)?;
    encoder.finish().map_err(tar_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_stream() {
        let body = concat!(
            "{\"command\":{\"id\":\"cmd_1\",\"exitCode\":null}}\n",
            "{\"stream\":\"stdout\",\"data\":\"hello \"}\n",
            "{\"stream\":\"stdout\",\"data\":\"world\\n\"}\n",
            "{\"stream\":\"stderr\",\"data\":\"warning\\n\"}\n",
            "{\"command\":{\"id\":\"cmd_1\",\"exitCode\":0}}\n",
        );
        let outcome = parse_command_stream(body);
        assert_eq!(outcome.stdout, "hello world\n");
        assert_eq!(outcome.stderr, "warning\n");
        assert_eq!(outcome.exit_code, Some(0));
        assert!(outcome.error.is_none());
    }

    #[test]
    fn test_parse_command_stream_error_line() {
        let body = "{\"stream\":\"error\",\"data\":{\"code\":\"sandbox_stream_closed\",\"message\":\"stream closed\"}}\n";
        let outcome = parse_command_stream(body);
        assert_eq!(outcome.error.as_deref(), Some("stream closed"));
        assert_eq!(outcome.exit_code, None);
    }

    #[test]
    fn test_build_tar_gz_roundtrip() {
        let archive = build_tar_gz("dir/test.txt", b"hello", 0o644).unwrap();
        // Gzip magic bytes.
        assert_eq!(&archive[..2], &[0x1f, 0x8b]);

        let decoder = flate2::read::GzDecoder::new(archive.as_slice());
        let mut tar = tar::Archive::new(decoder);
        let mut entries = tar.entries().unwrap();
        let mut entry = entries.next().unwrap().unwrap();
        assert_eq!(entry.path().unwrap().to_str().unwrap(), "dir/test.txt");
        assert_eq!(entry.header().mode().unwrap(), 0o644);
        use std::io::Read;
        let mut content = Vec::new();
        entry.read_to_end(&mut content).unwrap();
        assert_eq!(content, b"hello");
    }
}
