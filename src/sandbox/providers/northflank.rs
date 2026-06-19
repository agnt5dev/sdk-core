//! Northflank sandbox provider — REST lifecycle + websocket exec.
//!
//! Northflank has no dedicated sandbox API; following their sandboxes guide,
//! a "sandbox" is a deployment service running `sleep infinity`, created via
//! the REST API (`https://api.northflank.com/v1`, `Authorization: Bearer`)
//! and driven via the command-exec websocket
//! (`wss://api.northflank.com/v1/command-exec/projects/{p}/services/{s}`).
//!
//! The websocket protocol matches the official `@northflank/js-client`:
//! auth is in-band (first message is an `init` frame carrying the API
//! token), then `stdOut`/`stdErr` frames stream output and a `completion`
//! frame delivers the exit code.
//!
//! File operations have no native API and are implemented over exec
//! (base64 round-trips), so very large files should go through object
//! storage instead. Responses from the REST API are wrapped in `{"data": ...}`.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::providers::common::{
    interpreter_argv, parse_listing_output, provider_http_error, provider_transport_error,
    shell_single_quote,
};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxBackend, SandboxExecutor, SandboxProvider, SandboxWorkspace};
use async_trait::async_trait;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const PROVIDER: &str = "northflank";
const DEFAULT_BASE_URL: &str = "https://api.northflank.com";
const DEFAULT_PLAN: &str = "nf-compute-200";
const DEFAULT_IMAGE: &str = "python:3.12-slim-bookworm";

// ── Config ──────────────────────────────────────────────────────

/// Configuration for the Northflank provider.
#[derive(Debug, Clone)]
pub struct NorthflankProviderConfig {
    /// API token (personal or team token with API role).
    pub api_token: String,
    /// Project that sandbox services are created in.
    pub project_id: String,
    /// Team ID — required for team-scoped tokens (adds the `teams/{id}/`
    /// path prefix on exec connections).
    pub team_id: Option<String>,
    /// API base URL. Default: `https://api.northflank.com`.
    pub base_url: String,
    /// Compute plan for sandbox services. Default: `nf-compute-200`.
    pub deployment_plan: String,
    /// Default container image when [`CreateSandboxOptions::template`] is
    /// unset. Default: `python:3.12-slim-bookworm`.
    pub image: String,
    /// HTTP request timeout for non-execution calls.
    pub timeout: Duration,
    /// How long to wait for a new service's deployment to come up
    /// (includes image pull).
    pub ready_timeout: Duration,
}

impl NorthflankProviderConfig {
    /// Build configuration from `NORTHFLANK_API_TOKEN` and
    /// `NORTHFLANK_PROJECT_ID` (+ optional `NORTHFLANK_TEAM_ID`,
    /// `NORTHFLANK_API_URL`, `NORTHFLANK_DEPLOYMENT_PLAN`,
    /// `NORTHFLANK_SANDBOX_IMAGE`).
    pub fn from_env() -> Result<Self> {
        let api_token =
            std::env::var("NORTHFLANK_API_TOKEN").map_err(|_| SdkError::Configuration {
                message: "NORTHFLANK_API_TOKEN is required for the Northflank provider".to_string(),
                field: Some("NORTHFLANK_API_TOKEN".to_string()),
            })?;
        let project_id =
            std::env::var("NORTHFLANK_PROJECT_ID").map_err(|_| SdkError::Configuration {
                message: "NORTHFLANK_PROJECT_ID is required for the Northflank provider"
                    .to_string(),
                field: Some("NORTHFLANK_PROJECT_ID".to_string()),
            })?;
        Ok(Self {
            api_token,
            project_id,
            team_id: std::env::var("NORTHFLANK_TEAM_ID").ok(),
            base_url: std::env::var("NORTHFLANK_API_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.into()),
            deployment_plan: std::env::var("NORTHFLANK_DEPLOYMENT_PLAN")
                .unwrap_or_else(|_| DEFAULT_PLAN.into()),
            image: std::env::var("NORTHFLANK_SANDBOX_IMAGE")
                .unwrap_or_else(|_| DEFAULT_IMAGE.into()),
            timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(180),
        })
    }
}

// ── Wire types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct ServiceObject {
    id: String,
    #[serde(default)]
    status: Option<serde_json::Value>,
}

/// Extract a human-readable deployment status from the service status blob.
fn deployment_status(status: Option<&serde_json::Value>) -> String {
    let Some(status) = status else {
        return "unknown".to_string();
    };
    let deployment = status.get("deployment").unwrap_or(status);
    deployment
        .get("status")
        .and_then(|s| s.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| deployment.to_string())
}

/// Build the exec websocket URL, including the team prefix when present.
fn exec_ws_url(
    base_url: &str,
    team_id: Option<&str>,
    project_id: &str,
    service_id: &str,
) -> String {
    let ws_base = base_url
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let team_prefix = team_id.map(|t| format!("teams/{}/", t)).unwrap_or_default();
    format!(
        "{}/v1/command-exec/{}projects/{}/services/{}",
        ws_base, team_prefix, project_id, service_id
    )
}

// ── Provider (control plane) ────────────────────────────────────

/// Control plane for Northflank sandbox services.
pub struct NorthflankSandboxProvider {
    config: NorthflankProviderConfig,
    client: reqwest::Client,
}

impl NorthflankSandboxProvider {
    pub fn new(config: NorthflankProviderConfig) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", config.api_token))
                .map_err(|e| SdkError::Configuration {
                    message: format!("invalid Northflank API token: {}", e),
                    field: Some("api_token".to_string()),
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
        Self::new(NorthflankProviderConfig::from_env()?)
    }

    fn service_url(&self, service_id: &str) -> String {
        format!(
            "{}/v1/projects/{}/services/{}",
            self.config.base_url, self.config.project_id, service_id
        )
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

    fn handle(&self, service_id: String) -> NorthflankSandbox {
        NorthflankSandbox {
            client: self.client.clone(),
            config: self.config.clone(),
            service_id,
            capabilities: SandboxCapabilities {
                languages: vec![Language::Python, Language::Javascript, Language::Bash],
                supports_commands: true,
                supports_streaming: true,
                supports_git: false,
                supports_preview_url: false,
                supports_snapshots: false,
                max_execution_time_ms: 0,
                max_memory_bytes: 0,
                has_network_access: true,
            },
        }
    }

    /// Create a sandbox service, returning the concrete handle type.
    ///
    /// `cpu_cores`/`memory_mib` are ignored — Northflank sizes services via
    /// the billing plan (`deployment_plan` in the config).
    pub async fn create(&self, opts: CreateSandboxOptions) -> Result<NorthflankSandbox> {
        let name = format!("agnt5-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
        let image = opts.template.as_deref().unwrap_or(&self.config.image);
        let mut body = serde_json::json!({
            "name": name,
            "billing": { "deploymentPlan": self.config.deployment_plan },
            "deployment": {
                "external": { "imagePath": image },
                "docker": {
                    "configType": "customCommand",
                    "customCommand": "sleep infinity"
                }
            }
        });
        if let Some(env) = &opts.env {
            body["runtimeEnvironment"] = serde_json::json!(env);
        }

        let created: Envelope<ServiceObject> = self
            .send(
                "create_sandbox",
                self.client
                    .post(format!(
                        "{}/v1/projects/{}/services/deployment",
                        self.config.base_url, self.config.project_id
                    ))
                    .json(&body),
            )
            .await?;
        let service_id = created.data.id;

        // Wait for the deployment to come up (includes the image pull).
        let deadline = Instant::now() + self.config.ready_timeout;
        loop {
            let service: Envelope<ServiceObject> = self
                .send(
                    "get_service",
                    self.client.get(self.service_url(&service_id)),
                )
                .await?;
            let status = deployment_status(service.data.status.as_ref()).to_lowercase();
            if status.contains("running") || status.contains("completed") {
                break;
            }
            if status.contains("failed") || status.contains("error") {
                return Err(SdkError::Sandbox {
                    message: format!(
                        "Northflank service {} deployment failed (status: {})",
                        service_id, status
                    ),
                    operation: "create_sandbox".to_string(),
                    code: ErrorCode::SandboxUnavailable,
                });
            }
            if Instant::now() >= deadline {
                return Err(SdkError::Sandbox {
                    message: format!(
                        "Northflank service {} not running within {:?} (status: {})",
                        service_id, self.config.ready_timeout, status
                    ),
                    operation: "create_sandbox".to_string(),
                    code: ErrorCode::ExecutionTimeout,
                });
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        Ok(self.handle(service_id))
    }

    /// Connect to an existing sandbox service by ID.
    pub async fn connect(&self, service_id: &str) -> Result<NorthflankSandbox> {
        let _: Envelope<ServiceObject> = self
            .send(
                "connect_sandbox",
                self.client.get(self.service_url(service_id)),
            )
            .await?;
        Ok(self.handle(service_id.to_string()))
    }
}

#[async_trait]
impl SandboxProvider for NorthflankSandboxProvider {
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
                self.client.delete(self.service_url(sandbox_id)),
            )
            .await?;
        Ok(true)
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>> {
        let value: serde_json::Value = self
            .send(
                "list_sandboxes",
                self.client.get(format!(
                    "{}/v1/projects/{}/services",
                    self.config.base_url, self.config.project_id
                )),
            )
            .await?;
        let services = value
            .get("data")
            .and_then(|d| d.get("services"))
            .and_then(|s| s.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(services
            .iter()
            .map(|svc| SandboxInfo {
                sandbox_id: svc
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                status: deployment_status(svc.get("status")),
                backend_kind: SandboxBackendKind::Remote,
            })
            .collect())
    }
}

// ── Sandbox handle (data plane) ─────────────────────────────────

/// A running Northflank sandbox service.
pub struct NorthflankSandbox {
    client: reqwest::Client,
    config: NorthflankProviderConfig,
    service_id: String,
    capabilities: SandboxCapabilities,
}

impl NorthflankSandbox {
    /// Provider-native service ID.
    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// Run a command over the command-exec websocket.
    pub async fn run_command(&self, req: RunCommandRequest) -> Result<RunCommandResult> {
        // The exec context has no cwd/env parameters; wrap in a shell when
        // either is requested.
        let mut argv: Vec<String> = vec![req.command.clone()];
        argv.extend(req.args.clone().unwrap_or_default());
        if req.working_dir.is_some() || req.env.is_some() {
            let mut line = String::new();
            if let Some(dir) = &req.working_dir {
                line.push_str(&format!("cd {} && ", shell_single_quote(dir)));
            }
            if let Some(env) = &req.env {
                for (key, value) in env {
                    line.push_str(&format!("export {}={} && ", key, shell_single_quote(value)));
                }
            }
            line.push_str(
                &argv
                    .iter()
                    .map(|a| shell_single_quote(a))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            argv = vec!["bash".to_string(), "-c".to_string(), line];
        }
        self.exec_ws(argv, req.timeout_ms).await
    }

    async fn exec_ws(&self, argv: Vec<String>, timeout_ms: u64) -> Result<RunCommandResult> {
        let url = exec_ws_url(
            &self.config.base_url,
            self.config.team_id.as_deref(),
            &self.config.project_id,
            &self.service_id,
        );
        let started = Instant::now();

        let run = async {
            let (mut ws, _) =
                tokio_tungstenite::connect_async(&url)
                    .await
                    .map_err(|e| SdkError::Sandbox {
                        message: format!("websocket connect failed: {}", e),
                        operation: "run_command".to_string(),
                        code: ErrorCode::SandboxUnavailable,
                    })?;

            let init = serde_json::json!({
                "type": "init",
                "data": {
                    "auth": { "type": "apiToken", "apiToken": self.config.api_token },
                    "context": { "command": argv }
                }
            });
            ws.send(Message::Text(init.to_string().into()))
                .await
                .map_err(|e| SdkError::Sandbox {
                    message: format!("websocket send failed: {}", e),
                    operation: "run_command".to_string(),
                    code: ErrorCode::SandboxUnavailable,
                })?;

            let mut stdout = String::new();
            let mut stderr = String::new();
            let mut exit_code: Option<i32> = None;

            while let Some(frame) = ws.next().await {
                let frame = frame.map_err(|e| SdkError::Sandbox {
                    message: format!("websocket receive failed: {}", e),
                    operation: "run_command".to_string(),
                    code: ErrorCode::SandboxUnavailable,
                })?;
                let Message::Text(text) = frame else {
                    if matches!(frame, Message::Close(_)) {
                        break;
                    }
                    continue;
                };
                let Ok(msg) = serde_json::from_str::<serde_json::Value>(text.as_str()) else {
                    continue;
                };
                match msg.get("type").and_then(|t| t.as_str()) {
                    Some("init") => {
                        let auth = msg
                            .get("data")
                            .and_then(|d| d.get("auth"))
                            .and_then(|a| a.as_str())
                            .unwrap_or("");
                        if auth != "successful" {
                            return Err(SdkError::Sandbox {
                                message: format!("exec authentication failed: {}", msg),
                                operation: "run_command".to_string(),
                                code: ErrorCode::InvalidConfiguration,
                            });
                        }
                    }
                    Some("stdOut") => {
                        if let Some(data) = msg.get("data").and_then(|d| d.as_str()) {
                            stdout.push_str(data);
                        }
                    }
                    Some("stdErr") => {
                        if let Some(data) = msg.get("data").and_then(|d| d.as_str()) {
                            stderr.push_str(data);
                        }
                    }
                    Some("completion") => {
                        exit_code = msg
                            .get("data")
                            .and_then(|d| d.get("exitCode"))
                            .and_then(|c| c.as_i64())
                            .map(|c| c as i32);
                        break;
                    }
                    Some("error") => {
                        let message = msg
                            .get("data")
                            .and_then(|d| d.get("message"))
                            .and_then(|m| m.as_str())
                            .unwrap_or("unknown exec error")
                            .to_string();
                        return Err(SdkError::Sandbox {
                            message,
                            operation: "run_command".to_string(),
                            code: ErrorCode::SandboxExecutionFailed,
                        });
                    }
                    _ => {}
                }
            }

            Ok(RunCommandResult {
                stdout,
                stderr,
                exit_code: exit_code.unwrap_or(-1),
                execution_time_ms: started.elapsed().as_millis() as u64,
                error: None,
            })
        };

        tokio::time::timeout(Duration::from_millis(timeout_ms), run)
            .await
            .map_err(|_| SdkError::Sandbox {
                message: format!("command timed out after {} ms", timeout_ms),
                operation: "run_command".to_string(),
                code: ErrorCode::ExecutionTimeout,
            })?
    }
}

#[async_trait]
impl SandboxExecutor for NorthflankSandbox {
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
        let service: Envelope<ServiceObject> = {
            let resp = self
                .client
                .get(format!(
                    "{}/v1/projects/{}/services/{}",
                    self.config.base_url, self.config.project_id, self.service_id
                ))
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
        Ok(SandboxHealthResult {
            status: deployment_status(service.data.status.as_ref()),
            sandbox_id: self.service_id.clone(),
            uptime_ms: 0,
            backend_kind: SandboxBackendKind::Remote,
            error: None,
        })
    }
}

#[async_trait]
impl SandboxWorkspace for NorthflankSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        let size = req.content.len() as u64;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&req.content);
        let script = format!(
            "mkdir -p {parent} && printf '%s' {b64} | base64 -d > {path} && chmod {mode:o} {path}",
            parent = shell_single_quote(parent_dir(&req.path)),
            b64 = shell_single_quote(&encoded),
            path = shell_single_quote(&req.path),
            mode = req.mode,
        );
        let result = self
            .run_command(RunCommandRequest {
                command: "bash".to_string(),
                args: Some(vec!["-c".to_string(), script]),
                working_dir: None,
                env: None,
                timeout_ms: 60_000,
            })
            .await?;
        if result.exit_code != 0 {
            return Err(SdkError::Sandbox {
                message: format!("write_file failed: {}", result.stderr),
                operation: "write_file".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            });
        }
        Ok(WriteFileResult {
            success: true,
            path: req.path,
            size,
            error: None,
        })
    }

    async fn read_file(&self, path: &str) -> Result<ReadFileResult> {
        let result = self
            .run_command(RunCommandRequest {
                command: "base64".to_string(),
                args: Some(vec![path.to_string()]),
                working_dir: None,
                env: None,
                timeout_ms: 60_000,
            })
            .await?;
        if result.exit_code != 0 {
            return Err(SdkError::Sandbox {
                message: format!("read_file failed: {}", result.stderr),
                operation: "read_file".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            });
        }
        let cleaned: String = result
            .stdout
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let content = base64::engine::general_purpose::STANDARD
            .decode(&cleaned)
            .map_err(|e| SdkError::Sandbox {
                message: format!("invalid base64 from sandbox: {}", e),
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

/// Parent directory of a path, defaulting to "." for bare filenames.
fn parent_dir(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some(("", _)) => "/",
        Some((parent, _)) => parent,
        None => ".",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exec_ws_url() {
        assert_eq!(
            exec_ws_url("https://api.northflank.com", None, "proj", "svc"),
            "wss://api.northflank.com/v1/command-exec/projects/proj/services/svc"
        );
        assert_eq!(
            exec_ws_url("https://api.northflank.com", Some("team"), "proj", "svc"),
            "wss://api.northflank.com/v1/command-exec/teams/team/projects/proj/services/svc"
        );
    }

    #[test]
    fn test_deployment_status() {
        let status = serde_json::json!({ "deployment": { "status": "RUNNING" } });
        assert_eq!(deployment_status(Some(&status)), "RUNNING");
        let flat = serde_json::json!({ "status": "PAUSED" });
        assert_eq!(deployment_status(Some(&flat)), "PAUSED");
        assert_eq!(deployment_status(None), "unknown");
    }

    #[test]
    fn test_parent_dir() {
        assert_eq!(parent_dir("/workspace/app/main.py"), "/workspace/app");
        assert_eq!(parent_dir("/main.py"), "/");
        assert_eq!(parent_dir("main.py"), ".");
    }

    #[test]
    fn test_envelope_parse() {
        let json = r#"{"data":{"id":"svc-1","status":{"deployment":{"status":"RUNNING"}}}}"#;
        let envelope: Envelope<ServiceObject> = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.data.id, "svc-1");
        assert_eq!(deployment_status(envelope.data.status.as_ref()), "RUNNING");
    }
}
