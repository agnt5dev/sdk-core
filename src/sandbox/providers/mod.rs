//! Sandbox backend implementations.

pub(crate) mod common;
pub mod remote;

#[cfg(feature = "wasm-sandbox")]
pub mod wasm;
