//! MCP Client implementation
//!
//! The MCPClient connects to one or more MCP servers and provides a unified
//! interface for discovering and calling tools.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::mcp::error::{McpError, McpResult};
use crate::mcp::transport::{SseTransport, StdioTransport, Transport};
use crate::mcp::types::{
    CallToolParams, CallToolResult, InitializeParams, InitializeResult, JsonRpcRequest,
    ListToolsResult, McpTool, ServerCapabilities, ServerConfig, SseConfig, StdioConfig,
};

/// MCP Client for connecting to external MCP servers
///
/// The client manages connections to one or more MCP servers and provides
/// a unified interface for discovering and calling tools.
///
/// # Example
///
/// ```rust,ignore
/// use agnt5_sdk_core::mcp::{McpClient, ServerConfig, StdioConfig};
///
/// let mut client = McpClient::new("my-client");
///
/// // Add a Wikipedia MCP server
/// client.add_server(
///     "wikipedia",
///     ServerConfig::Stdio(StdioConfig::new("npx", vec!["-y".into(), "wikipedia-mcp".into()])),
/// );
///
/// // Connect to all servers
/// client.connect().await?;
///
/// // List available tools
/// let tools = client.list_tools().await?;
///
/// // Call a tool
/// let result = client.call_tool("wikipedia", "search", serde_json::json!({"query": "Rust"})).await?;
/// ```
pub struct McpClient {
    /// Client identifier
    id: String,
    /// Server configurations (name -> config)
    server_configs: HashMap<String, ServerConfig>,
    /// Active server connections (name -> connection)
    connections: Arc<RwLock<HashMap<String, ServerConnection>>>,
}

/// Active connection to an MCP server
struct ServerConnection {
    transport: Box<dyn Transport>,
    capabilities: ServerCapabilities,
    tools: Vec<McpTool>,
    initialized: bool,
}

impl McpClient {
    /// Create a new MCP client
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            server_configs: HashMap::new(),
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the client ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Add a server configuration
    pub fn add_server(&mut self, name: impl Into<String>, config: ServerConfig) {
        self.server_configs.insert(name.into(), config);
    }

    /// Add a stdio server (convenience method)
    pub fn add_stdio_server(
        &mut self,
        name: impl Into<String>,
        command: impl Into<String>,
        args: Vec<String>,
    ) {
        self.add_server(
            name,
            ServerConfig::Stdio(StdioConfig::new(command, args)),
        );
    }

    /// Add an SSE server (convenience method)
    pub fn add_sse_server(&mut self, name: impl Into<String>, url: impl Into<String>) {
        self.add_server(name, ServerConfig::Sse(SseConfig::new(url)));
    }

    /// Add an SSE server with API key (convenience method)
    pub fn add_sse_server_with_api_key(
        &mut self,
        name: impl Into<String>,
        url: impl Into<String>,
        api_key: impl Into<String>,
    ) {
        self.add_server(
            name,
            ServerConfig::Sse(SseConfig::new(url).with_api_key(api_key)),
        );
    }

    /// Connect to all configured servers
    pub async fn connect(&self) -> McpResult<()> {
        for (name, config) in &self.server_configs {
            self.connect_server(name, config.clone()).await?;
        }
        Ok(())
    }

    /// Connect to a specific server
    async fn connect_server(&self, name: &str, config: ServerConfig) -> McpResult<()> {
        tracing::info!("Connecting to MCP server: {}", name);

        // Create transport based on config
        let transport: Box<dyn Transport> = match config {
            ServerConfig::Stdio(stdio_config) => {
                Box::new(StdioTransport::new(stdio_config).await?)
            }
            ServerConfig::Sse(sse_config) => {
                Box::new(SseTransport::new(sse_config).await?)
            }
        };

        // Initialize the connection
        let init_params = InitializeParams::default();
        let init_req = JsonRpcRequest::new(
            "initialize",
            Some(serde_json::to_value(&init_params)?),
            0,
        );

        let init_response = transport.request(init_req).await?;
        let init_result: InitializeResult = serde_json::from_value(
            init_response
                .into_result()
                .map_err(|e| McpError::Server(e.to_string()))?,
        )?;

        tracing::info!(
            "Connected to MCP server {} (version: {}, protocol: {})",
            init_result.server_info.name,
            init_result.server_info.version,
            init_result.protocol_version
        );

        // Send initialized notification
        let initialized_req = JsonRpcRequest::notification("initialized", None);
        transport.notify(initialized_req).await?;

        // List available tools
        let tools = if init_result.capabilities.tools.is_some() {
            let tools_req = JsonRpcRequest::new("tools/list", None, 0);
            let tools_response = transport.request(tools_req).await?;
            let tools_result: ListToolsResult = serde_json::from_value(
                tools_response
                    .into_result()
                    .map_err(|e| McpError::Server(e.to_string()))?,
            )?;
            tools_result.tools
        } else {
            Vec::new()
        };

        tracing::info!(
            "MCP server {} has {} tools available",
            name,
            tools.len()
        );

        // Store connection
        let connection = ServerConnection {
            transport,
            capabilities: init_result.capabilities,
            tools,
            initialized: true,
        };

        let mut connections = self.connections.write().await;
        connections.insert(name.to_string(), connection);

        Ok(())
    }

    /// Disconnect from all servers
    pub async fn disconnect(&self) -> McpResult<()> {
        let mut connections = self.connections.write().await;
        for (name, conn) in connections.drain() {
            tracing::info!("Disconnecting from MCP server: {}", name);
            let _ = conn.transport.close().await;
        }
        Ok(())
    }

    /// List all available tools from all connected servers
    pub async fn list_tools(&self) -> McpResult<Vec<McpToolWithServer>> {
        let connections = self.connections.read().await;
        let mut all_tools = Vec::new();

        for (server_name, conn) in connections.iter() {
            for tool in &conn.tools {
                all_tools.push(McpToolWithServer {
                    server: server_name.clone(),
                    tool: tool.clone(),
                });
            }
        }

        Ok(all_tools)
    }

    /// List tools from a specific server
    pub async fn list_server_tools(&self, server: &str) -> McpResult<Vec<McpTool>> {
        let connections = self.connections.read().await;
        let conn = connections
            .get(server)
            .ok_or_else(|| McpError::Server(format!("Server not connected: {}", server)))?;

        Ok(conn.tools.clone())
    }

    /// Call a tool on a specific server
    pub async fn call_tool(
        &self,
        server: &str,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> McpResult<CallToolResult> {
        let connections = self.connections.read().await;
        let conn = connections
            .get(server)
            .ok_or_else(|| McpError::Server(format!("Server not connected: {}", server)))?;

        // Verify tool exists
        if !conn.tools.iter().any(|t| t.name == tool_name) {
            return Err(McpError::ToolNotFound(tool_name.to_string()));
        }

        let params = CallToolParams {
            name: tool_name.to_string(),
            arguments: if arguments.is_null() {
                None
            } else {
                Some(arguments)
            },
        };

        let req = JsonRpcRequest::new("tools/call", Some(serde_json::to_value(&params)?), 0);
        let response = conn.transport.request(req).await?;

        let result: CallToolResult = serde_json::from_value(
            response
                .into_result()
                .map_err(|e| McpError::Server(e.to_string()))?,
        )?;

        Ok(result)
    }

    /// Call a tool by finding it across all servers
    pub async fn call_tool_auto(
        &self,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> McpResult<CallToolResult> {
        // Find the server that has this tool
        let server_name = {
            let connections = self.connections.read().await;
            let mut found_server = None;
            for (name, conn) in connections.iter() {
                if conn.tools.iter().any(|t| t.name == tool_name) {
                    found_server = Some(name.clone());
                    break;
                }
            }
            found_server
        };

        if let Some(server) = server_name {
            self.call_tool(&server, tool_name, arguments).await
        } else {
            Err(McpError::ToolNotFound(tool_name.to_string()))
        }
    }

    /// Get server capabilities
    pub async fn get_capabilities(&self, server: &str) -> McpResult<ServerCapabilities> {
        let connections = self.connections.read().await;
        let conn = connections
            .get(server)
            .ok_or_else(|| McpError::Server(format!("Server not connected: {}", server)))?;

        Ok(conn.capabilities.clone())
    }

    /// Check if a server is connected
    pub async fn is_connected(&self, server: &str) -> bool {
        let connections = self.connections.read().await;
        if let Some(conn) = connections.get(server) {
            conn.initialized && conn.transport.is_connected()
        } else {
            false
        }
    }

    /// Get list of connected servers
    pub async fn connected_servers(&self) -> Vec<String> {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect()
    }
}

/// MCP tool with server information
#[derive(Debug, Clone)]
pub struct McpToolWithServer {
    /// Server name
    pub server: String,
    /// Tool definition
    pub tool: McpTool,
}

impl Drop for McpClient {
    fn drop(&mut self) {
        // Note: async drop is not supported, so cleanup happens in disconnect()
        // Users should call disconnect() explicitly for clean shutdown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = McpClient::new("test-client");
        assert_eq!(client.id(), "test-client");
    }

    #[test]
    fn test_add_servers() {
        let mut client = McpClient::new("test");
        client.add_stdio_server("wikipedia", "npx", vec!["-y".into(), "wikipedia-mcp".into()]);
        client.add_sse_server("remote", "https://example.com/mcp");

        assert_eq!(client.server_configs.len(), 2);
    }
}
