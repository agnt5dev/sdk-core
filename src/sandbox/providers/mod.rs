//! Sandbox backend implementations.
//!
//! - [`remote`][] — generic HTTP client for any AGNT5-protocol sandbox server
//! - [`wasm`][] — embedded Wasmtime backend (feature `wasm-sandbox`)
//!
pub mod remote;

#[cfg(feature = "wasm-sandbox")]
pub mod wasm;
