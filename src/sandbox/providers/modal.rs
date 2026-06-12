//! Modal Sandboxes provider — placeholder pending native gRPC support.
//!
//! Modal exposes no REST API: all clients speak gRPC/protobuf to the
//! `modal.client.ModalClient` service at `api.modal.com:443`, authenticated
//! via `x-modal-token-id` / `x-modal-token-secret` metadata. Creating a
//! sandbox additionally requires orchestrating prerequisite RPCs
//! (`AppGetOrCreate` → `ImageGetOrCreate` + `ImageJoinStreaming` →
//! `SandboxCreate`), and command execution uses server-streaming RPCs —
//! optionally over a second gRPC channel directly to the worker.
//!
//! A native integration therefore means vendoring Modal's proto definitions
//! and generating a tonic client — planned as a follow-up. Until then this
//! module reserves the provider surface and fails fast with clear guidance
//! instead of silently misbehaving.

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxBackend, SandboxProvider};
use async_trait::async_trait;
use std::sync::Arc;

const PROVIDER: &str = "modal";

/// Configuration for the Modal provider.
#[derive(Debug, Clone)]
pub struct ModalProviderConfig {
    /// Modal token ID (`ak-...`).
    pub token_id: String,
    /// Modal token secret (`as-...`).
    pub token_secret: String,
}

impl ModalProviderConfig {
    /// Build configuration from `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET`.
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
        })
    }
}

/// Control plane for Modal Sandboxes (not yet implemented — see module docs).
pub struct ModalSandboxProvider {
    #[allow(dead_code)]
    config: ModalProviderConfig,
}

impl ModalSandboxProvider {
    pub fn new(config: ModalProviderConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub fn from_env() -> Result<Self> {
        Self::new(ModalProviderConfig::from_env()?)
    }

    fn not_implemented(operation: &str) -> SdkError {
        SdkError::Sandbox {
            message: "Modal's API is gRPC-only (modal.client.ModalClient); native support \
                      requires a tonic client and is not yet implemented. Use Modal's own \
                      SDK, or another sandbox provider (e2b, daytona, vercel, northflank, \
                      together) in the meantime."
                .to_string(),
            operation: operation.to_string(),
            code: ErrorCode::NotImplemented,
        }
    }
}

#[async_trait]
impl SandboxProvider for ModalSandboxProvider {
    fn name(&self) -> &'static str {
        PROVIDER
    }

    async fn create_sandbox(&self, _opts: CreateSandboxOptions) -> Result<Arc<dyn SandboxBackend>> {
        Err(Self::not_implemented("create_sandbox"))
    }

    async fn connect_sandbox(&self, _sandbox_id: &str) -> Result<Arc<dyn SandboxBackend>> {
        Err(Self::not_implemented("connect_sandbox"))
    }

    async fn destroy_sandbox(&self, _sandbox_id: &str) -> Result<bool> {
        Err(Self::not_implemented("destroy_sandbox"))
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>> {
        Err(Self::not_implemented("list_sandboxes"))
    }
}
