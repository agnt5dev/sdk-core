//! Sandbox backend implementations.
//!
//! - [`remote`][] — generic HTTP client for any AGNT5-protocol sandbox server
//! - [`wasm`][] — embedded Wasmtime backend (feature `wasm-sandbox`)
//!
//! Managed provider integrations (control plane + data plane):
//!
//! - [`e2b`][] — E2B (api.e2b.app + envd/code-interpreter data plane)
//! - [`daytona`][] — Daytona (app.daytona.io + toolbox proxy)
//! - [`vercel`][] — Vercel Sandbox (api.vercel.com /v2/sandboxes)
//! - [`northflank`][] — Northflank (REST lifecycle + websocket exec)
//! - [`together`][] — Together Code Interpreter (api.together.ai /v1/tci)
//! - [`modal`][] — Modal (native gRPC via vendored protos)

pub(crate) mod common;
pub mod daytona;
pub mod e2b;
pub mod modal;
pub mod northflank;
pub mod remote;
pub mod together;
pub mod vercel;

#[cfg(feature = "wasm-sandbox")]
pub mod wasm;
