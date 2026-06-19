//! Modal Sandboxes provider — native gRPC integration.
//!
//! Modal exposes no REST API: all clients speak gRPC/protobuf to the
//! `modal.client.ModalClient` service at `api.modal.com:443`. This module
//! uses a tonic client generated from the vendored contract in
//! `proto/modal/api.proto` (taken from `modal-labs/modal-client`).
//!
//! # Lifecycle
//!
//! Creating a sandbox orchestrates Modal's prerequisite objects:
//!
//! 1. `AppGetOrCreate` — sandboxes live under an app (default
//!    `agnt5-sandboxes`), created on first use.
//! 2. `ImageGetOrCreate` + `ImageJoinStreaming` — the image is defined by
//!    dockerfile commands (default `FROM python:3.12-slim`) and built
//!    server-side; the streaming RPC waits for the build. Builds are cached
//!    by recipe, so subsequent creates are fast.
//! 3. `SandboxCreate` — the sandbox runs `sleep 172800` as its entrypoint.
//!
//! Command execution resolves the sandbox's task via `SandboxGetTaskId`,
//! then uses `ContainerExec` + the server-streaming
//! `ContainerExecGetOutput` (one stream per file descriptor) and
//! `ContainerExecWait` for the exit code. File operations are emulated over
//! exec (base64 round-trips), like the Northflank provider.
//!
//! # Auth
//!
//! Token id/secret are sent as `x-modal-token-id` / `x-modal-token-secret`
//! metadata. A short-lived JWT is fetched via `AuthTokenGet` and attached
//! as `x-modal-auth-token`; if that RPC fails the provider proceeds on
//! token auth alone.
//!
//! # Caveats
//!
//! - `ContainerExecRequest` has no env-var field; `env` on requests is
//!   emulated by prefixing the command with `env K=V ...`.
//! - `CreateSandboxOptions::metadata` maps to Modal sandbox tags.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::providers::common::{
    interpreter_argv, parse_listing_output, shell_single_quote,
};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxBackend, SandboxExecutor, SandboxProvider, SandboxWorkspace};
use async_trait::async_trait;
use base64::Engine;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig};

/// Generated from the vendored `proto/modal/api.proto`. The full contract
/// is compiled; only the sandbox-related messages are constructed, so dead
/// code is expected.
#[allow(dead_code, clippy::all)]
pub(crate) mod proto {
    tonic::include_proto!("modal.client");
}

use proto::modal_client_client::ModalClientClient;

const PROVIDER: &str = "modal";
const DEFAULT_SERVER_URL: &str = "https://api.modal.com:443";
const DEFAULT_APP_NAME: &str = "agnt5-sandboxes";
const DEFAULT_IMAGE: &str = "python:3.12-slim";
/// `x-modal-client-version` — the server gates on Python SDK versions, so we
/// present a recent one (override with `MODAL_CLIENT_VERSION` if Modal
/// starts rejecting it).
const DEFAULT_CLIENT_VERSION: &str = "1.1.0";
/// `ClientType::Client` — the Modal Python SDK client type.
const CLIENT_TYPE: &str = "1";
/// Sandbox entrypoint: idle until commands arrive (48h, like Modal's SDKs).
const ENTRYPOINT_SLEEP_SECS: &str = "172800";
/// Long-poll window for streaming RPCs; re-issued until the deadline.
const POLL_WINDOW_SECS: f32 = 55.0;

// ── Config ──────────────────────────────────────────────────────

/// Configuration for the Modal provider.
#[derive(Debug, Clone)]
pub struct ModalProviderConfig {
    /// Modal token ID (`ak-...`).
    pub token_id: String,
    /// Modal token secret (`as-...`).
    pub token_secret: String,
    /// gRPC endpoint. Default: `https://api.modal.com:443`.
    pub server_url: String,
    /// Modal environment name (empty = workspace default).
    pub environment_name: String,
    /// App that sandboxes are created under. Default: `agnt5-sandboxes`.
    pub app_name: String,
    /// Default image when [`CreateSandboxOptions::template`] is unset.
    /// Either an image reference or full dockerfile commands (`FROM ...`).
    pub image: String,
    /// Version presented as `x-modal-client-version`.
    pub client_version: String,
    /// Timeout for unary RPCs.
    pub timeout: Duration,
    /// How long to wait for image builds (first create of a new image).
    pub build_timeout: Duration,
}

impl ModalProviderConfig {
    /// Build configuration from `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET`
    /// (+ optional `MODAL_SERVER_URL`, `MODAL_ENVIRONMENT`,
    /// `MODAL_APP_NAME`, `MODAL_SANDBOX_IMAGE`, `MODAL_CLIENT_VERSION`).
    pub fn from_env() -> Result<Self> {
        let token_id = std::env::var("MODAL_TOKEN_ID").map_err(|_| SdkError::Configuration {
            message: "MODAL_TOKEN_ID is required for the Modal provider".to_string(),
            field: Some("MODAL_TOKEN_ID".to_string()),
        })?;
        let token_secret =
            std::env::var("MODAL_TOKEN_SECRET").map_err(|_| SdkError::Configuration {
                message: "MODAL_TOKEN_SECRET is required for the Modal provider".to_string(),
                field: Some("MODAL_TOKEN_SECRET".to_string()),
            })?;
        Ok(Self {
            token_id,
            token_secret,
            server_url: std::env::var("MODAL_SERVER_URL")
                .unwrap_or_else(|_| DEFAULT_SERVER_URL.into()),
            environment_name: std::env::var("MODAL_ENVIRONMENT").unwrap_or_default(),
            app_name: std::env::var("MODAL_APP_NAME").unwrap_or_else(|_| DEFAULT_APP_NAME.into()),
            image: std::env::var("MODAL_SANDBOX_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.into()),
            client_version: std::env::var("MODAL_CLIENT_VERSION")
                .unwrap_or_else(|_| DEFAULT_CLIENT_VERSION.into()),
            timeout: Duration::from_secs(60),
            build_timeout: Duration::from_secs(300),
        })
    }
}

// ── Auth interceptor ────────────────────────────────────────────

struct AuthState {
    token_id: AsciiMetadataValue,
    token_secret: AsciiMetadataValue,
    client_version: AsciiMetadataValue,
    auth_token: std::sync::RwLock<Option<AsciiMetadataValue>>,
}

#[derive(Clone)]
struct ModalAuth {
    state: Arc<AuthState>,
}

impl tonic::service::Interceptor for ModalAuth {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        let md = req.metadata_mut();
        md.insert("x-modal-token-id", self.state.token_id.clone());
        md.insert("x-modal-token-secret", self.state.token_secret.clone());
        md.insert(
            "x-modal-client-type",
            AsciiMetadataValue::from_static(CLIENT_TYPE),
        );
        md.insert("x-modal-client-version", self.state.client_version.clone());
        if let Some(token) = self
            .state
            .auth_token
            .read()
            .expect("auth token lock poisoned")
            .as_ref()
        {
            md.insert("x-modal-auth-token", token.clone());
        }
        Ok(req)
    }
}

type GrpcClient = ModalClientClient<InterceptedService<Channel, ModalAuth>>;

// ── Helpers ─────────────────────────────────────────────────────

fn ascii_value(value: &str, field: &str) -> Result<AsciiMetadataValue> {
    value.parse().map_err(|_| SdkError::Configuration {
        message: format!("{} contains characters not valid in gRPC metadata", field),
        field: Some(field.to_string()),
    })
}

fn grpc_error(operation: &str, status: tonic::Status) -> SdkError {
    let code = match status.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
            ErrorCode::InvalidConfiguration
        }
        tonic::Code::Unavailable => ErrorCode::SandboxUnavailable,
        tonic::Code::DeadlineExceeded => ErrorCode::ExecutionTimeout,
        tonic::Code::NotFound => ErrorCode::SandboxUnavailable,
        _ => ErrorCode::SandboxExecutionFailed,
    };
    SdkError::Sandbox {
        message: format!("Modal gRPC error ({}): {}", status.code(), status.message()),
        operation: operation.to_string(),
        code,
    }
}

/// Dockerfile commands for a template: image references become a single
/// `FROM`, while templates that already look like dockerfile content are
/// passed through line by line.
fn dockerfile_commands(template: &str) -> Vec<String> {
    let trimmed = template.trim();
    if trimmed.contains('\n') || trimmed.to_ascii_uppercase().starts_with("FROM ") {
        trimmed.lines().map(|l| l.trim().to_string()).collect()
    } else {
        vec![format!("FROM {}", trimmed)]
    }
}

/// Human-readable label for a `GenericResult` status.
fn status_label(status: i32) -> &'static str {
    use proto::generic_result::GenericStatus;
    match GenericStatus::try_from(status) {
        Ok(GenericStatus::Success) => "success",
        Ok(GenericStatus::Failure) => "failure",
        Ok(GenericStatus::Terminated) => "terminated",
        Ok(GenericStatus::Timeout) => "timeout",
        Ok(GenericStatus::InitFailure) => "init_failure",
        Ok(GenericStatus::InternalFailure) => "internal_failure",
        Ok(GenericStatus::IdleTimeout) => "idle_timeout",
        _ => "unknown",
    }
}

/// Build the exec argv, emulating env vars via `env K=V ...` since
/// `ContainerExecRequest` has no env field.
fn exec_argv(
    command: &str,
    args: Option<&Vec<String>>,
    env: Option<&std::collections::HashMap<String, String>>,
) -> Vec<String> {
    let mut argv = Vec::new();
    if let Some(env) = env {
        if !env.is_empty() {
            argv.push("env".to_string());
            for (key, value) in env {
                argv.push(format!("{}={}", key, value));
            }
        }
    }
    argv.push(command.to_string());
    if let Some(args) = args {
        argv.extend(args.iter().cloned());
    }
    argv
}

fn text_of(message: &proto::RuntimeOutputMessage) -> String {
    if !message.message.is_empty() {
        message.message.clone()
    } else {
        String::from_utf8_lossy(&message.message_bytes).into_owned()
    }
}

// ── Provider (control plane) ────────────────────────────────────

/// Control plane for Modal Sandboxes.
pub struct ModalSandboxProvider {
    config: ModalProviderConfig,
    client: GrpcClient,
    auth_state: Arc<AuthState>,
    auth_init: tokio::sync::OnceCell<()>,
    app_id: tokio::sync::OnceCell<String>,
}

impl ModalSandboxProvider {
    pub fn new(config: ModalProviderConfig) -> Result<Self> {
        let auth_state = Arc::new(AuthState {
            token_id: ascii_value(&config.token_id, "token_id")?,
            token_secret: ascii_value(&config.token_secret, "token_secret")?,
            client_version: ascii_value(&config.client_version, "client_version")?,
            auth_token: std::sync::RwLock::new(None),
        });

        let mut endpoint = Channel::from_shared(config.server_url.clone()).map_err(|e| {
            SdkError::Configuration {
                message: format!("invalid Modal server URL: {}", e),
                field: Some("server_url".to_string()),
            }
        })?;
        endpoint = endpoint.connect_timeout(Duration::from_secs(15));
        if config.server_url.starts_with("https://") {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|e| SdkError::Configuration {
                    message: format!("failed to configure TLS: {}", e),
                    field: None,
                })?;
        }
        let channel = endpoint.connect_lazy();
        let client = ModalClientClient::with_interceptor(
            channel,
            ModalAuth {
                state: auth_state.clone(),
            },
        );

        Ok(Self {
            config,
            client,
            auth_state,
            auth_init: tokio::sync::OnceCell::new(),
            app_id: tokio::sync::OnceCell::new(),
        })
    }

    /// Build the provider from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::new(ModalProviderConfig::from_env()?)
    }

    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(message);
        req.set_timeout(self.config.timeout);
        req
    }

    /// Fetch the short-lived auth JWT once. Failure is tolerated — the
    /// token id/secret metadata may be sufficient on their own.
    async fn ensure_auth(&self) {
        self.auth_init
            .get_or_init(|| async {
                let mut client = self.client.clone();
                match client
                    .auth_token_get(self.request(proto::AuthTokenGetRequest {}))
                    .await
                {
                    Ok(resp) => {
                        let token = resp.into_inner().token;
                        if let Ok(value) = token.parse::<AsciiMetadataValue>() {
                            *self
                                .auth_state
                                .auth_token
                                .write()
                                .expect("auth token lock poisoned") = Some(value);
                        }
                    }
                    Err(status) => {
                        tracing::debug!(
                            "Modal AuthTokenGet failed ({}); continuing with token auth",
                            status.code()
                        );
                    }
                }
            })
            .await;
    }

    async fn ensure_app(&self) -> Result<String> {
        self.ensure_auth().await;
        self.app_id
            .get_or_try_init(|| async {
                let mut client = self.client.clone();
                let resp = client
                    .app_get_or_create(self.request(proto::AppGetOrCreateRequest {
                        app_name: self.config.app_name.clone(),
                        environment_name: self.config.environment_name.clone(),
                        object_creation_type: proto::ObjectCreationType::CreateIfMissing as i32,
                    }))
                    .await
                    .map_err(|s| grpc_error("create_sandbox", s))?;
                Ok(resp.into_inner().app_id)
            })
            .await
            .cloned()
    }

    /// Get-or-create the image and wait for its build to complete.
    /// Builds are cached by recipe server-side, so repeat calls are cheap.
    async fn ensure_image(&self, app_id: &str, template: Option<&str>) -> Result<String> {
        let mut client = self.client.clone();
        let resp = client
            .image_get_or_create(self.request(proto::ImageGetOrCreateRequest {
                image: Some(proto::Image {
                    dockerfile_commands: dockerfile_commands(
                        template.unwrap_or(&self.config.image),
                    ),
                    ..Default::default()
                }),
                app_id: app_id.to_string(),
                namespace: proto::DeploymentNamespace::Workspace as i32,
                ..Default::default()
            }))
            .await
            .map_err(|s| grpc_error("create_sandbox", s))?
            .into_inner();

        let image_id = resp.image_id;
        if let Some(result) = resp.result {
            return check_build_result(&image_id, &result);
        }

        // Build in progress: poll the streaming join until it completes.
        let deadline = Instant::now() + self.config.build_timeout;
        while Instant::now() < deadline {
            let mut stream = client
                .image_join_streaming(self.long_request(proto::ImageJoinStreamingRequest {
                    image_id: image_id.clone(),
                    timeout: POLL_WINDOW_SECS,
                    ..Default::default()
                }))
                .await
                .map_err(|s| grpc_error("create_sandbox", s))?
                .into_inner();
            while let Some(msg) = stream
                .message()
                .await
                .map_err(|s| grpc_error("create_sandbox", s))?
            {
                if let Some(result) = msg.result {
                    if result.status != 0 {
                        return check_build_result(&image_id, &result);
                    }
                }
            }
        }
        Err(SdkError::Sandbox {
            message: format!(
                "Modal image {} not built within {:?}",
                image_id, self.config.build_timeout
            ),
            operation: "create_sandbox".to_string(),
            code: ErrorCode::ExecutionTimeout,
        })
    }

    /// Request with headroom for one long-poll window.
    fn long_request<T>(&self, message: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(message);
        req.set_timeout(Duration::from_secs_f32(POLL_WINDOW_SECS + 15.0));
        req
    }

    fn handle(&self, sandbox_id: String) -> ModalSandbox {
        ModalSandbox {
            client: self.client.clone(),
            sandbox_id,
            task_id: tokio::sync::OnceCell::new(),
            unary_timeout: self.config.timeout,
            capabilities: SandboxCapabilities {
                languages: vec![Language::Python, Language::Javascript, Language::Bash],
                supports_commands: true,
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

    /// Create a sandbox, returning the concrete handle type.
    pub async fn create(&self, opts: CreateSandboxOptions) -> Result<ModalSandbox> {
        let app_id = self.ensure_app().await?;
        let image_id = self.ensure_image(&app_id, opts.template.as_deref()).await?;

        let resources = if opts.cpu_cores.is_some() || opts.memory_mib.is_some() {
            Some(proto::Resources {
                memory_mb: opts.memory_mib.unwrap_or(0) as u32,
                milli_cpu: opts.cpu_cores.map(|c| c * 1000).unwrap_or(0),
                ..Default::default()
            })
        } else {
            None
        };
        let tags = opts
            .metadata
            .iter()
            .flatten()
            .map(|(k, v)| proto::SandboxTag {
                tag_name: k.clone(),
                tag_value: v.clone(),
            })
            .collect();

        let mut client = self.client.clone();
        let resp = client
            .sandbox_create(self.request(proto::SandboxCreateRequest {
                app_id,
                definition: Some(proto::Sandbox {
                    entrypoint_args: vec!["sleep".to_string(), ENTRYPOINT_SLEEP_SECS.to_string()],
                    image_id,
                    timeout_secs: opts.timeout_secs.unwrap_or(300) as u32,
                    resources,
                    ..Default::default()
                }),
                tags,
                ..Default::default()
            }))
            .await
            .map_err(|s| grpc_error("create_sandbox", s))?;
        Ok(self.handle(resp.into_inner().sandbox_id))
    }

    /// Connect to an existing sandbox by ID.
    pub async fn connect(&self, sandbox_id: &str) -> Result<ModalSandbox> {
        self.ensure_auth().await;
        // Verify the sandbox exists and is still running.
        let mut client = self.client.clone();
        let resp = client
            .sandbox_wait(self.request(proto::SandboxWaitRequest {
                sandbox_id: sandbox_id.to_string(),
                timeout: 0.0,
            }))
            .await
            .map_err(|s| grpc_error("connect_sandbox", s))?;
        if let Some(result) = resp.into_inner().result {
            return Err(SdkError::Sandbox {
                message: format!(
                    "Modal sandbox {} already finished ({})",
                    sandbox_id,
                    status_label(result.status)
                ),
                operation: "connect_sandbox".to_string(),
                code: ErrorCode::SandboxUnavailable,
            });
        }
        Ok(self.handle(sandbox_id.to_string()))
    }
}

fn check_build_result(image_id: &str, result: &proto::GenericResult) -> Result<String> {
    use proto::generic_result::GenericStatus;
    if result.status == GenericStatus::Success as i32 {
        Ok(image_id.to_string())
    } else {
        Err(SdkError::Sandbox {
            message: format!(
                "Modal image build failed ({}): {}",
                status_label(result.status),
                result.exception
            ),
            operation: "create_sandbox".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })
    }
}

#[async_trait]
impl SandboxProvider for ModalSandboxProvider {
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
        self.ensure_auth().await;
        let mut client = self.client.clone();
        client
            .sandbox_terminate(self.request(proto::SandboxTerminateRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .map_err(|s| grpc_error("destroy_sandbox", s))?;
        Ok(true)
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>> {
        let app_id = self.ensure_app().await?;
        let mut client = self.client.clone();
        let resp = client
            .sandbox_list(self.request(proto::SandboxListRequest {
                app_id,
                environment_name: self.config.environment_name.clone(),
                include_finished: false,
                ..Default::default()
            }))
            .await
            .map_err(|s| grpc_error("list_sandboxes", s))?;
        Ok(resp
            .into_inner()
            .sandboxes
            .into_iter()
            .map(|s| {
                let status = s
                    .task_info
                    .and_then(|t| t.result)
                    .map(|r| status_label(r.status).to_string())
                    .unwrap_or_else(|| "running".to_string());
                SandboxInfo {
                    sandbox_id: s.id,
                    status,
                    backend_kind: SandboxBackendKind::Remote,
                }
            })
            .collect())
    }
}

// ── Sandbox handle (data plane) ─────────────────────────────────

/// A running Modal sandbox.
pub struct ModalSandbox {
    client: GrpcClient,
    sandbox_id: String,
    task_id: tokio::sync::OnceCell<String>,
    unary_timeout: Duration,
    capabilities: SandboxCapabilities,
}

impl ModalSandbox {
    /// Provider-native sandbox ID.
    pub fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(message);
        req.set_timeout(self.unary_timeout);
        req
    }

    async fn ensure_task_id(&self) -> Result<String> {
        self.task_id
            .get_or_try_init(|| async {
                let mut client = self.client.clone();
                let mut req = tonic::Request::new(proto::SandboxGetTaskIdRequest {
                    sandbox_id: self.sandbox_id.clone(),
                    timeout: Some(POLL_WINDOW_SECS),
                    wait_until_ready: false,
                });
                req.set_timeout(Duration::from_secs_f32(POLL_WINDOW_SECS + 15.0));
                let resp = client
                    .sandbox_get_task_id(req)
                    .await
                    .map_err(|s| grpc_error("run_command", s))?
                    .into_inner();
                resp.task_id.ok_or_else(|| SdkError::Sandbox {
                    message: format!(
                        "Modal sandbox {} has no task (terminated before scheduling?)",
                        self.sandbox_id
                    ),
                    operation: "run_command".to_string(),
                    code: ErrorCode::SandboxUnavailable,
                })
            })
            .await
            .cloned()
    }

    /// Collect one output stream (stdout or stderr) for an exec, re-issuing
    /// the long-poll until the stream reports an exit code or the deadline.
    async fn collect_output(
        &self,
        exec_id: &str,
        file_descriptor: proto::FileDescriptor,
        deadline: Instant,
    ) -> Result<(String, Option<i32>)> {
        let mut output = String::new();
        let mut exit_code: Option<i32> = None;
        let mut last_batch_index = 0u64;
        let mut client = self.client.clone();

        while exit_code.is_none() && Instant::now() < deadline {
            let mut req = tonic::Request::new(proto::ContainerExecGetOutputRequest {
                exec_id: exec_id.to_string(),
                timeout: POLL_WINDOW_SECS,
                last_batch_index,
                file_descriptor: file_descriptor as i32,
                get_raw_bytes: false,
            });
            req.set_timeout(Duration::from_secs_f32(POLL_WINDOW_SECS + 15.0));
            let mut stream = client
                .container_exec_get_output(req)
                .await
                .map_err(|s| grpc_error("run_command", s))?
                .into_inner();

            let mut saw_message = false;
            while let Some(batch) = stream
                .message()
                .await
                .map_err(|s| grpc_error("run_command", s))?
            {
                saw_message = true;
                last_batch_index = batch.batch_index;
                for item in &batch.items {
                    output.push_str(&text_of(item));
                }
                for item in &batch.stdout {
                    output.push_str(&text_of(item));
                }
                for item in &batch.stderr {
                    output.push_str(&text_of(item));
                }
                if batch.exit_code.is_some() {
                    exit_code = batch.exit_code;
                }
            }
            if exit_code.is_some() {
                break;
            }
            if !saw_message {
                // Don't hot-loop if the server returns empty streams.
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        Ok((output, exit_code))
    }

    /// Run a command in the sandbox's container.
    pub async fn run_command(&self, req: RunCommandRequest) -> Result<RunCommandResult> {
        let task_id = self.ensure_task_id().await?;
        let started = Instant::now();
        let timeout_secs = (req.timeout_ms / 1000).max(1) as u32;
        let deadline = started + Duration::from_millis(req.timeout_ms.saturating_add(30_000));

        let mut client = self.client.clone();
        let exec_id = client
            .container_exec(self.request(proto::ContainerExecRequest {
                task_id,
                command: exec_argv(&req.command, req.args.as_ref(), req.env.as_ref()),
                stdout_output: proto::ExecOutputOption::Pipe as i32,
                stderr_output: proto::ExecOutputOption::Pipe as i32,
                timeout_secs,
                workdir: req.working_dir.clone(),
                ..Default::default()
            }))
            .await
            .map_err(|s| grpc_error("run_command", s))?
            .into_inner()
            .exec_id;

        // Output is buffered server-side per batch index, so the two file
        // descriptors can be drained sequentially.
        let (stdout, exit_a) = self
            .collect_output(&exec_id, proto::FileDescriptor::Stdout, deadline)
            .await?;
        let (stderr, exit_b) = self
            .collect_output(&exec_id, proto::FileDescriptor::Stderr, deadline)
            .await?;

        let mut exit_code = exit_a.or(exit_b);
        if exit_code.is_none() {
            let resp = client
                .container_exec_wait(self.request(proto::ContainerExecWaitRequest {
                    exec_id,
                    timeout: 5.0,
                }))
                .await
                .map_err(|s| grpc_error("run_command", s))?
                .into_inner();
            exit_code = resp.exit_code;
        }

        Ok(RunCommandResult {
            stdout,
            stderr,
            exit_code: exit_code.unwrap_or(-1),
            execution_time_ms: started.elapsed().as_millis() as u64,
            error: None,
        })
    }
}

#[async_trait]
impl SandboxExecutor for ModalSandbox {
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
        let mut client = self.client.clone();
        let resp = client
            .sandbox_wait(self.request(proto::SandboxWaitRequest {
                sandbox_id: self.sandbox_id.clone(),
                timeout: 0.0,
            }))
            .await
            .map_err(|s| grpc_error("health", s))?
            .into_inner();
        let status = match resp.result {
            None => "running".to_string(),
            Some(result) => status_label(result.status).to_string(),
        };
        Ok(SandboxHealthResult {
            status,
            sandbox_id: self.sandbox_id.clone(),
            uptime_ms: 0,
            backend_kind: SandboxBackendKind::Remote,
            error: None,
        })
    }
}

#[async_trait]
impl SandboxWorkspace for ModalSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        let size = req.content.len() as u64;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&req.content);
        let parent = match req.path.rsplit_once('/') {
            Some(("", _)) => "/",
            Some((parent, _)) => parent,
            None => ".",
        };
        let script = format!(
            "mkdir -p {parent} && printf '%s' {b64} | base64 -d > {path} && chmod {mode:o} {path}",
            parent = shell_single_quote(parent),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dockerfile_commands() {
        assert_eq!(
            dockerfile_commands("python:3.12-slim"),
            vec!["FROM python:3.12-slim"]
        );
        assert_eq!(
            dockerfile_commands("FROM node:22-slim"),
            vec!["FROM node:22-slim"]
        );
        assert_eq!(
            dockerfile_commands("FROM python:3.12-slim\nRUN pip install requests"),
            vec!["FROM python:3.12-slim", "RUN pip install requests"]
        );
    }

    #[test]
    fn test_status_label() {
        use proto::generic_result::GenericStatus;
        assert_eq!(status_label(GenericStatus::Success as i32), "success");
        assert_eq!(status_label(GenericStatus::Failure as i32), "failure");
        assert_eq!(status_label(GenericStatus::Timeout as i32), "timeout");
        assert_eq!(status_label(999), "unknown");
    }

    #[test]
    fn test_exec_argv_env_emulation() {
        let mut env = std::collections::HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        let argv = exec_argv("python3", Some(&vec!["-c".into(), "x".into()]), Some(&env));
        assert_eq!(argv, vec!["env", "FOO=bar", "python3", "-c", "x"]);

        let argv = exec_argv("ls", None, None);
        assert_eq!(argv, vec!["ls"]);
    }

    #[test]
    fn test_sandbox_definition_construction() {
        // Sanity-check the generated proto types and field names.
        let definition = proto::Sandbox {
            entrypoint_args: vec!["sleep".into(), ENTRYPOINT_SLEEP_SECS.into()],
            image_id: "im-123".into(),
            timeout_secs: 300,
            resources: Some(proto::Resources {
                memory_mb: 1024,
                milli_cpu: 2000,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(definition.image_id, "im-123");
        assert_eq!(definition.resources.unwrap().milli_cpu, 2000);
    }

    #[tokio::test]
    async fn test_provider_construction_is_lazy() {
        // Construction needs a runtime (connect_lazy spawns its connector
        // task) but must not hit the network.
        let provider = ModalSandboxProvider::new(ModalProviderConfig {
            token_id: "ak-test".into(),
            token_secret: "as-test".into(),
            server_url: DEFAULT_SERVER_URL.into(),
            environment_name: String::new(),
            app_name: DEFAULT_APP_NAME.into(),
            image: DEFAULT_IMAGE.into(),
            client_version: DEFAULT_CLIENT_VERSION.into(),
            timeout: Duration::from_secs(60),
            build_timeout: Duration::from_secs(300),
        });
        assert!(provider.is_ok());
        assert_eq!(provider.unwrap().name(), "modal");
    }
}
