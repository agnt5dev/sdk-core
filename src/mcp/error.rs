//! MCP-specific error types

use crate::mcp::types::JsonRpcError;
use thiserror::Error;

/// Errors that can occur during MCP operations
#[derive(Error, Debug)]
pub enum McpError {
    /// Transport-level error (connection, I/O, etc.)
    #[error("Transport error: {0}")]
    Transport(String),

    /// JSON-RPC protocol error
    #[error("JSON-RPC error: {0}")]
    JsonRpc(#[from] JsonRpcError),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Protocol error (invalid response, etc.)
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Server not initialized
    #[error("Server not initialized")]
    NotInitialized,

    /// Tool not found
    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    /// Server error (from MCP server)
    #[error("Server error: {0}")]
    Server(String),

    /// Timeout error
    #[error("Operation timed out")]
    Timeout,

    /// Connection closed
    #[error("Connection closed")]
    ConnectionClosed,

    /// HTTP error
    #[error("HTTP error: {0}")]
    Http(String),

    /// Process spawn error
    #[error("Failed to spawn process: {0}")]
    ProcessSpawn(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<reqwest::Error> for McpError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            McpError::Timeout
        } else if err.is_connect() {
            McpError::Transport(format!("Connection failed: {}", err))
        } else {
            McpError::Http(err.to_string())
        }
    }
}

/// Result type for MCP operations
pub type McpResult<T> = Result<T, McpError>;
