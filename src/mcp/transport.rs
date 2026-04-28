//! MCP transport implementations
//!
//! This module provides transport layer abstractions for MCP communication.
//! Supported transports:
//! - Stdio: For local subprocess communication (npx, uvx, etc.)
//! - SSE: For HTTP-based servers (remote MCP servers, AGNT5 Gateway)

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex};

use crate::mcp::error::{McpError, McpResult};
use crate::mcp::types::{JsonRpcRequest, JsonRpcResponse, SseConfig, StdioConfig};

/// Transport trait for MCP communication
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send a request and wait for response
    async fn request(&self, req: JsonRpcRequest) -> McpResult<JsonRpcResponse>;

    /// Send a notification (no response expected)
    async fn notify(&self, req: JsonRpcRequest) -> McpResult<()>;

    /// Close the transport
    async fn close(&self) -> McpResult<()>;

    /// Check if transport is connected
    fn is_connected(&self) -> bool;
}

// ============================================================================
// Stdio Transport
// ============================================================================

/// Stdio transport for local subprocess communication
///
/// Uses Content-Length framing (LSP/MCP standard) for message boundaries.
pub struct StdioTransport {
    /// Child process handle
    process: Arc<Mutex<Option<Child>>>,
    /// Stdin writer
    stdin: Arc<Mutex<Option<tokio::process::ChildStdin>>>,
    /// Pending responses channel
    pending_responses: Arc<Mutex<HashMap<u64, mpsc::Sender<JsonRpcResponse>>>>,
    /// Request ID counter
    request_id: AtomicU64,
    /// Connected state
    connected: AtomicBool,
    /// Background reader task handle
    _reader_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl StdioTransport {
    /// Create a new stdio transport from config
    pub async fn new(config: StdioConfig) -> McpResult<Self> {
        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args);

        // Set environment variables
        for (key, value) in &config.env {
            cmd.env(key, value);
        }

        // Set working directory if specified
        if let Some(cwd) = &config.cwd {
            cmd.current_dir(cwd);
        }

        // Configure stdio
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        // Spawn the process
        let mut child = cmd.spawn().map_err(|e| {
            McpError::ProcessSpawn(format!(
                "{} {}: {}",
                config.command,
                config.args.join(" "),
                e
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::Transport("Failed to get stdin handle".to_string()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::Transport("Failed to get stdout handle".to_string()))?;

        let pending_responses: Arc<Mutex<HashMap<u64, mpsc::Sender<JsonRpcResponse>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = Arc::clone(&pending_responses);
        let connected = Arc::new(AtomicBool::new(true));
        let connected_clone = Arc::clone(&connected);

        // Start background reader task
        let reader_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                match read_message(&mut reader).await {
                    Ok(Some(data)) => {
                        match serde_json::from_slice::<JsonRpcResponse>(&data) {
                            Ok(response) => {
                                // Route response to waiting caller
                                if let Some(id) = extract_response_id(&response) {
                                    let mut pending = pending_clone.lock().await;
                                    if let Some(sender) = pending.remove(&id) {
                                        let _ = sender.send(response).await;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to parse response: {}", e);
                            }
                        }
                    }
                    Ok(None) => {
                        // EOF - process exited
                        connected_clone.store(false, Ordering::SeqCst);
                        break;
                    }
                    Err(e) => {
                        tracing::error!("Error reading from process: {}", e);
                        connected_clone.store(false, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            process: Arc::new(Mutex::new(Some(child))),
            stdin: Arc::new(Mutex::new(Some(stdin))),
            pending_responses,
            request_id: AtomicU64::new(1),
            connected: AtomicBool::new(true),
            _reader_handle: Arc::new(Mutex::new(Some(reader_handle))),
        })
    }

    /// Get next request ID
    fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl Transport for StdioTransport {
    async fn request(&self, mut req: JsonRpcRequest) -> McpResult<JsonRpcResponse> {
        if !self.is_connected() {
            return Err(McpError::ConnectionClosed);
        }

        // Assign request ID
        let id = self.next_id();
        req.id = crate::mcp::types::JsonRpcId::Number(id);

        // Create response channel
        let (tx, mut rx) = mpsc::channel(1);
        {
            let mut pending = self.pending_responses.lock().await;
            pending.insert(id, tx);
        }

        // Send request
        let message = serde_json::to_vec(&req)?;
        {
            let mut stdin_guard = self.stdin.lock().await;
            if let Some(stdin) = stdin_guard.as_mut() {
                write_message(stdin, &message).await?;
            } else {
                return Err(McpError::ConnectionClosed);
            }
        }

        // Wait for response with timeout
        match tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv()).await {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(McpError::ConnectionClosed),
            Err(_) => {
                // Clean up pending request
                let mut pending = self.pending_responses.lock().await;
                pending.remove(&id);
                Err(McpError::Timeout)
            }
        }
    }

    async fn notify(&self, req: JsonRpcRequest) -> McpResult<()> {
        if !self.is_connected() {
            return Err(McpError::ConnectionClosed);
        }

        let message = serde_json::to_vec(&req)?;
        let mut stdin_guard = self.stdin.lock().await;
        if let Some(stdin) = stdin_guard.as_mut() {
            write_message(stdin, &message).await?;
        } else {
            return Err(McpError::ConnectionClosed);
        }

        Ok(())
    }

    async fn close(&self) -> McpResult<()> {
        self.connected.store(false, Ordering::SeqCst);

        // Close stdin
        {
            let mut stdin_guard = self.stdin.lock().await;
            *stdin_guard = None;
        }

        // Kill process
        {
            let mut process_guard = self.process.lock().await;
            if let Some(mut child) = process_guard.take() {
                let _ = child.kill().await;
            }
        }

        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

/// Read a Content-Length framed message
async fn read_message<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> McpResult<Option<Vec<u8>>> {
    // Read headers
    let mut content_length: Option<usize> = None;
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            return Ok(None); // EOF
        }

        let line = line.trim();
        if line.is_empty() {
            break; // End of headers
        }

        if let Some(value) = line.strip_prefix("Content-Length:") {
            content_length = Some(
                value
                    .trim()
                    .parse()
                    .map_err(|_| McpError::Protocol("Invalid Content-Length".to_string()))?,
            );
        }
    }

    let length = content_length
        .ok_or_else(|| McpError::Protocol("Missing Content-Length header".to_string()))?;

    // Read body
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await?;

    Ok(Some(body))
}

/// Write a Content-Length framed message
async fn write_message<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    message: &[u8],
) -> McpResult<()> {
    let header = format!("Content-Length: {}\r\n\r\n", message.len());
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(message).await?;
    writer.flush().await?;
    Ok(())
}

/// Extract numeric ID from response
fn extract_response_id(response: &JsonRpcResponse) -> Option<u64> {
    match &response.id {
        crate::mcp::types::JsonRpcId::Number(n) => Some(*n),
        _ => None,
    }
}

// ============================================================================
// SSE Transport
// ============================================================================

/// SSE transport for HTTP-based MCP servers
///
/// Uses Server-Sent Events for receiving responses and HTTP POST for requests.
pub struct SseTransport {
    /// Server URL
    url: String,
    /// HTTP client
    client: reqwest::Client,
    /// Request ID counter
    request_id: AtomicU64,
    /// Session ID (assigned by server)
    session_id: Arc<Mutex<Option<String>>>,
    /// Connected state (shared with background SSE reader task)
    connected_shared: Arc<AtomicBool>,
    /// Pending responses
    pending_responses: Arc<Mutex<HashMap<u64, mpsc::Sender<JsonRpcResponse>>>>,
}

impl SseTransport {
    /// Create a new SSE transport from config
    pub async fn new(config: SseConfig) -> McpResult<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, value) in &config.headers {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    .map_err(|e| McpError::Transport(format!("Invalid header name: {}", e)))?,
                reqwest::header::HeaderValue::from_str(value)
                    .map_err(|e| McpError::Transport(format!("Invalid header value: {}", e)))?,
            );
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| McpError::Transport(format!("Failed to create HTTP client: {}", e)))?;

        let transport = Self {
            url: config.url,
            client,
            request_id: AtomicU64::new(1),
            session_id: Arc::new(Mutex::new(None)),
            connected_shared: Arc::new(AtomicBool::new(false)),
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
        };

        // Connect to SSE stream
        transport.connect().await?;

        Ok(transport)
    }

    /// Connect to SSE stream
    async fn connect(&self) -> McpResult<()> {
        let url = format!("{}", self.url);
        let response = self
            .client
            .get(&url)
            .header("Accept", "text/event-stream")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(McpError::Http(format!(
                "SSE connection failed: {}",
                response.status()
            )));
        }

        let pending_clone = Arc::clone(&self.pending_responses);
        let session_clone = Arc::clone(&self.session_id);
        let connected_clone = Arc::clone(&self.connected_shared);

        // Start background SSE reader
        tokio::spawn(async move {
            use futures::StreamExt;

            let mut stream = response.bytes_stream();

            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        let text = String::from_utf8_lossy(&chunk);
                        for line in text.lines() {
                            if let Some(data) = line.strip_prefix("data: ") {
                                // Try to parse as JSON
                                if let Ok(value) = serde_json::from_str::<serde_json::Value>(data) {
                                    // Check for session ID
                                    if let Some(session) =
                                        value.get("session").and_then(|v| v.as_str())
                                    {
                                        let mut session_guard = session_clone.lock().await;
                                        *session_guard = Some(session.to_string());
                                    }

                                    // Check for JSON-RPC response
                                    if let Ok(response) =
                                        serde_json::from_value::<JsonRpcResponse>(value)
                                    {
                                        if let Some(id) = extract_response_id(&response) {
                                            let mut pending = pending_clone.lock().await;
                                            if let Some(sender) = pending.remove(&id) {
                                                let _ = sender.send(response).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("SSE stream error: {}", e);
                        connected_clone.store(false, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });

        self.connected_shared.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Get next request ID
    fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl Transport for SseTransport {
    async fn request(&self, mut req: JsonRpcRequest) -> McpResult<JsonRpcResponse> {
        if !self.is_connected() {
            return Err(McpError::ConnectionClosed);
        }

        // Assign request ID
        let id = self.next_id();
        req.id = crate::mcp::types::JsonRpcId::Number(id);

        // Create response channel
        let (tx, mut rx) = mpsc::channel(1);
        {
            let mut pending = self.pending_responses.lock().await;
            pending.insert(id, tx);
        }

        // Build URL with session if available
        let mut url = self.url.clone();
        {
            let session = self.session_id.lock().await;
            if let Some(sid) = session.as_ref() {
                if url.contains('?') {
                    url = format!("{}&session={}", url, sid);
                } else {
                    url = format!("{}?session={}", url, sid);
                }
            }
        }

        // Send request via POST
        let response = self.client.post(&url).json(&req).send().await?;

        if !response.status().is_success() {
            let mut pending = self.pending_responses.lock().await;
            pending.remove(&id);
            return Err(McpError::Http(format!(
                "Request failed: {}",
                response.status()
            )));
        }

        // Wait for response via SSE or HTTP response body
        // First try to get from HTTP response (for stateless endpoints)
        let body = response.text().await?;
        if !body.is_empty() {
            if let Ok(rpc_response) = serde_json::from_str::<JsonRpcResponse>(&body) {
                let mut pending = self.pending_responses.lock().await;
                pending.remove(&id);
                return Ok(rpc_response);
            }
        }

        // Wait for response via SSE with timeout
        match tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv()).await {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(McpError::ConnectionClosed),
            Err(_) => {
                let mut pending = self.pending_responses.lock().await;
                pending.remove(&id);
                Err(McpError::Timeout)
            }
        }
    }

    async fn notify(&self, req: JsonRpcRequest) -> McpResult<()> {
        if !self.is_connected() {
            return Err(McpError::ConnectionClosed);
        }

        // Build URL with session
        let mut url = self.url.clone();
        {
            let session = self.session_id.lock().await;
            if let Some(sid) = session.as_ref() {
                if url.contains('?') {
                    url = format!("{}&session={}", url, sid);
                } else {
                    url = format!("{}?session={}", url, sid);
                }
            }
        }

        let response = self.client.post(&url).json(&req).send().await?;

        if !response.status().is_success() {
            return Err(McpError::Http(format!(
                "Notification failed: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn close(&self) -> McpResult<()> {
        self.connected_shared.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected_shared.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stdio_config() {
        let config = StdioConfig::new("npx", vec!["-y".to_string(), "wikipedia-mcp".to_string()])
            .with_env("FOO", "bar")
            .with_cwd("/tmp");

        assert_eq!(config.command, "npx");
        assert_eq!(config.args.len(), 2);
        assert_eq!(config.env.get("FOO"), Some(&"bar".to_string()));
        assert_eq!(config.cwd, Some("/tmp".to_string()));
    }

    #[test]
    fn test_sse_config() {
        let config = SseConfig::new("https://example.com/mcp/sse").with_api_key("test-key");

        assert_eq!(config.url, "https://example.com/mcp/sse");
        assert_eq!(
            config.headers.get("X-API-KEY"),
            Some(&"test-key".to_string())
        );
    }
}
