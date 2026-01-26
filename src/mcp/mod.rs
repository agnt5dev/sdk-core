//! MCP (Model Context Protocol) support for AGNT5
//!
//! This module provides MCP client functionality for connecting to external
//! MCP servers and using their tools within AGNT5 agents.
//!
//! # Overview
//!
//! The Model Context Protocol (MCP) is a standard for AI agents to interact with
//! external tools and services. This module provides:
//!
//! - **MCPClient**: Connect to external MCP servers (Smithery, Composio, Wikipedia, etc.)
//! - **Transport abstraction**: Support for stdio (subprocess) and SSE (HTTP) transports
//! - **Tool discovery**: List and call tools from connected servers
//!
//! # Example
//!
//! ```rust,ignore
//! use agnt5_sdk_core::mcp::{McpClient, ServerConfig, StdioConfig, SseConfig};
//!
//! // Create client
//! let mut client = McpClient::new("research-client");
//!
//! // Add servers
//! client.add_stdio_server("wikipedia", "npx", vec!["-y".into(), "wikipedia-mcp".into()]);
//! client.add_sse_server_with_api_key("agnt5", "http://localhost:34183/v1/mcp/sse", "your-api-key");
//!
//! // Connect
//! client.connect().await?;
//!
//! // List tools from all servers
//! let tools = client.list_tools().await?;
//!
//! // Call a tool
//! let result = client.call_tool("wikipedia", "search", serde_json::json!({"query": "Rust"})).await?;
//!
//! // Disconnect when done
//! client.disconnect().await?;
//! ```
//!
//! # Transports
//!
//! ## Stdio Transport
//!
//! For local subprocess communication with MCP servers:
//!
//! ```rust,ignore
//! let config = StdioConfig::new("npx", vec!["-y".into(), "wikipedia-mcp".into()])
//!     .with_env("DEBUG", "true")
//!     .with_cwd("/tmp");
//! ```
//!
//! ## SSE Transport
//!
//! For HTTP-based MCP servers (including AGNT5 Gateway):
//!
//! ```rust,ignore
//! let config = SseConfig::new("https://smithery.ai/weather-mcp")
//!     .with_header("Authorization", "Bearer token")
//!     .with_api_key("your-api-key");
//! ```

mod client;
mod error;
mod transport;
mod types;

// Re-export main types
pub use client::{McpClient, McpToolWithServer};
pub use error::{McpError, McpResult};
pub use transport::{SseTransport, StdioTransport, Transport};
pub use types::{
    CallToolParams, CallToolResult, ClientCapabilities, ClientInfo, InitializeParams,
    InitializeResult, JsonRpcError, JsonRpcId, JsonRpcRequest, JsonRpcResponse, ListPromptsResult,
    ListResourcesResult, ListToolsResult, McpPrompt, McpResource, McpTool, PromptArgument,
    ResourceReference, ServerCapabilities, ServerConfig, ServerInfo, SseConfig, StdioConfig,
    ToolContent, ToolsCapability,
};
