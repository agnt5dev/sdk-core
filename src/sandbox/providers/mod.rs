//! Sandbox backend implementations.

pub mod remote;

#[cfg(feature = "wasm-sandbox")]
pub mod wasm;
