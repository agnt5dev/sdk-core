// Journal client for real-time SSE streaming of spans and logs
//
// This module provides a client to send span data directly to the AGNT5 journal
// via gRPC, bypassing the OTEL Collector entirely. This enables true real-time
// SSE streaming for interactive scenarios like watching agent execution live.
//
// IMPORTANT: This is NOT an OpenTelemetry SpanExporter. It's a simple gRPC client
// that should be called directly from worker code after streaming spans end.
// This avoids BatchSpanProcessor delays and Tokio runtime issues.

use crate::pb::{
    worker_coordinator_service_client::WorkerCoordinatorServiceClient,
    WriteJournalEventRequest,
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Journal-compatible span data structure.
/// This mirrors the SpanEventData type from platform/pkg/journal/types.go
#[derive(Debug, Clone, Serialize)]
pub struct JournalSpanData {
    pub trace_id: String,
    pub span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: String,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub status: JournalSpanStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<JournalSpanEvent>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<JournalSpanLink>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalSpanStatus {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalSpanEvent {
    pub name: String,
    pub timestamp_unix_nano: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalSpanLink {
    pub trace_id: String,
    pub span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

/// Client for exporting spans directly to the AGNT5 journal.
///
/// This is a simple gRPC client - NOT an OpenTelemetry SpanExporter.
/// Call `export_span` directly from worker code for real-time streaming.
#[derive(Debug, Clone)]
pub struct JournalClient {
    client: Arc<Mutex<Option<WorkerCoordinatorServiceClient<Channel>>>>,
    endpoint: String,
}

impl JournalClient {
    /// Create a new journal client.
    ///
    /// The client will lazily connect to the coordinator on first export.
    pub fn new(endpoint: String) -> Self {
        info!("🔍 STREAM-DEBUG: JournalClient created for endpoint: {}", endpoint);
        Self {
            client: Arc::new(Mutex::new(None)),
            endpoint,
        }
    }

    /// Create a journal client from environment variables.
    ///
    /// Uses AGNT5_COORDINATOR_ENDPOINT or defaults to http://localhost:34186
    pub fn from_env() -> Self {
        let endpoint = std::env::var("AGNT5_COORDINATOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:34186".to_string());
        Self::new(endpoint)
    }

    /// Ensure the gRPC client is connected.
    pub async fn ensure_connected(&self) -> Result<(), String> {
        let mut client_guard = self.client.lock().await;
        if client_guard.is_none() {
            debug!("Connecting journal client to {}", self.endpoint);
            match Channel::from_shared(self.endpoint.clone()) {
                Ok(channel_builder) => {
                    match channel_builder.connect().await {
                        Ok(channel) => {
                            info!("🔍 STREAM-DEBUG: JournalClient connected to {}", self.endpoint);
                            *client_guard = Some(WorkerCoordinatorServiceClient::new(channel));
                        }
                        Err(e) => {
                            warn!("Failed to connect journal client: {}", e);
                            return Err(format!("Connection failed: {}", e));
                        }
                    }
                }
                Err(e) => {
                    warn!("Invalid journal client endpoint: {}", e);
                    return Err(format!("Invalid endpoint: {}", e));
                }
            }
        }
        Ok(())
    }

    /// Export a single span to the journal.
    ///
    /// This sends the span data directly via gRPC - no batching, no delay.
    /// Call this immediately after a streaming span ends for real-time SSE delivery.
    ///
    /// # Arguments
    /// * `run_id` - The run ID to associate the span with
    /// * `span_data` - The span data to export
    /// * `tenant_id` - Optional tenant ID
    pub async fn export_span(
        &self,
        run_id: &str,
        span_data: &JournalSpanData,
        tenant_id: Option<&str>,
    ) -> Result<(), String> {
        info!(
            "🔍 STREAM-DEBUG: JournalClient.export_span called for run_id={}, span={}",
            run_id, span_data.name
        );

        // Ensure connected
        self.ensure_connected().await?;

        // Serialize span data
        let data = serde_json::to_vec(span_data)
            .map_err(|e| format!("Failed to serialize span: {}", e))?;

        // Build request with source timestamp for correct ordering
        // Use end_time_unix_nano as the source timestamp since that's when the span completed
        let request = WriteJournalEventRequest {
            run_id: run_id.to_string(),
            event_type: "span".to_string(),
            data,
            trace_id: span_data.trace_id.clone(),
            span_id: span_data.span_id.clone(),
            tenant_id: tenant_id.unwrap_or_default().to_string(),
            source_timestamp_ns: span_data.end_time_unix_nano,
        };

        // Send via gRPC
        let mut client_guard = self.client.lock().await;
        if let Some(ref mut grpc_client) = *client_guard {
            match grpc_client.write_journal_event(request).await {
                Ok(_response) => {
                    info!(
                        "🔍 STREAM-DEBUG: JournalClient.export_span SUCCESS for run_id={}, span={}",
                        run_id, span_data.name
                    );
                    Ok(())
                }
                Err(e) => {
                    error!(
                        "🔍 STREAM-DEBUG: JournalClient.export_span FAILED: {} for run_id={}, span={}",
                        e, run_id, span_data.name
                    );
                    Err(format!("gRPC call failed: {}", e))
                }
            }
        } else {
            Err("Client not connected".to_string())
        }
    }
}

/// Global journal client instance.
/// Initialized lazily on first use.
static JOURNAL_CLIENT: std::sync::OnceLock<JournalClient> = std::sync::OnceLock::new();

/// Get the global journal client instance.
///
/// Creates the client on first call using environment variables.
pub fn get_journal_client() -> &'static JournalClient {
    JOURNAL_CLIENT.get_or_init(JournalClient::from_env)
}

/// Export a span to the journal for real-time SSE streaming.
///
/// This is the main entry point for worker code to export spans.
/// Call this immediately after a streaming span ends.
///
/// # Arguments
/// * `run_id` - The run ID (invocation ID) to associate the span with
/// * `span_data` - The span data to export
/// * `tenant_id` - Optional tenant ID
///
/// # Example
/// ```ignore
/// // After span ends in worker code:
/// if is_streaming {
///     let span_data = create_journal_span_data(&span);
///     export_span_to_journal(&run_id, &span_data, Some(&tenant_id)).await;
/// }
/// ```
pub async fn export_span_to_journal(
    run_id: &str,
    span_data: &JournalSpanData,
    tenant_id: Option<&str>,
) -> Result<(), String> {
    get_journal_client().export_span(run_id, span_data, tenant_id).await
}

/// Journal-compatible log data structure.
#[derive(Debug, Clone, Serialize)]
pub struct JournalLogData {
    pub timestamp_unix_nano: i64,
    pub severity: String,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
    pub trace_id: String,
    pub span_id: String,
}

/// Write a generic event to the journal.
///
/// This is a general-purpose method for writing any event type to the journal,
/// including lm.call.*, output.*, and custom event types.
///
/// # Arguments
/// * `run_id` - The run ID to associate the event with
/// * `event_type` - Event type (e.g., "lm.call.started", "lm.call.completed")
/// * `data` - JSON-serialized event data
/// * `trace_id` - Trace ID for correlation
/// * `span_id` - Span ID for correlation
/// * `tenant_id` - Optional tenant ID
/// * `source_timestamp_ns` - Source timestamp in nanoseconds
pub async fn write_event(
    run_id: &str,
    event_type: &str,
    data: &[u8],
    trace_id: &str,
    span_id: &str,
    tenant_id: Option<&str>,
    source_timestamp_ns: i64,
) -> Result<(), String> {
    let client = get_journal_client();

    debug!(
        "write_event called: run_id={}, event_type={}, trace_id={}",
        run_id, event_type, trace_id
    );

    // Ensure connected
    client.ensure_connected().await?;

    // Build request
    let request = WriteJournalEventRequest {
        run_id: run_id.to_string(),
        event_type: event_type.to_string(),
        data: data.to_vec(),
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        tenant_id: tenant_id.unwrap_or_default().to_string(),
        source_timestamp_ns,
    };

    // Send via gRPC
    let mut client_guard = client.client.lock().await;
    if let Some(ref mut grpc_client) = *client_guard {
        match grpc_client.write_journal_event(request).await {
            Ok(_response) => {
                debug!(
                    "write_event SUCCESS: run_id={}, event_type={}",
                    run_id, event_type
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "write_event FAILED: {} for run_id={}, event_type={}",
                    e, run_id, event_type
                );
                Err(format!("gRPC call failed: {}", e))
            }
        }
    } else {
        Err("Client not connected".to_string())
    }
}

/// Export a log event to the journal for real-time SSE streaming.
///
/// This is the entry point for worker code to export logs.
/// Call this immediately after a log event for real-time SSE delivery.
///
/// # Arguments
/// * `run_id` - The run ID (invocation ID) to associate the log with
/// * `log_data` - The log data to export
/// * `tenant_id` - Optional tenant ID
pub async fn export_log_to_journal(
    run_id: &str,
    log_data: &JournalLogData,
    tenant_id: Option<&str>,
) -> Result<(), String> {
    let client = get_journal_client();

    info!(
        "🔍 STREAM-DEBUG: export_log_to_journal called for run_id={}, severity={}",
        run_id, log_data.severity
    );

    // Ensure connected
    client.ensure_connected().await?;

    // Serialize log data
    let data = serde_json::to_vec(log_data)
        .map_err(|e| format!("Failed to serialize log: {}", e))?;

    // Build request with source timestamp for correct ordering
    // Use timestamp_unix_nano as the source timestamp since that's when the log was generated
    let request = WriteJournalEventRequest {
        run_id: run_id.to_string(),
        event_type: "log".to_string(),
        data,
        trace_id: log_data.trace_id.clone(),
        span_id: log_data.span_id.clone(),
        tenant_id: tenant_id.unwrap_or_default().to_string(),
        source_timestamp_ns: log_data.timestamp_unix_nano,
    };

    // Send via gRPC
    let mut client_guard = client.client.lock().await;
    if let Some(ref mut grpc_client) = *client_guard {
        match grpc_client.write_journal_event(request).await {
            Ok(_response) => {
                info!(
                    "🔍 STREAM-DEBUG: export_log_to_journal SUCCESS for run_id={}",
                    run_id
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "🔍 STREAM-DEBUG: export_log_to_journal FAILED: {} for run_id={}",
                    e, run_id
                );
                Err(format!("gRPC call failed: {}", e))
            }
        }
    } else {
        Err("Client not connected".to_string())
    }
}

/// Create JournalLogData from log information.
///
/// Helper function to convert log data into journal format.
pub fn create_journal_log_data(
    timestamp_unix_nano: i64,
    severity: &str,
    body: &str,
    trace_id: &str,
    span_id: &str,
    attributes: Option<serde_json::Value>,
) -> JournalLogData {
    JournalLogData {
        timestamp_unix_nano,
        severity: severity.to_string(),
        body: body.to_string(),
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        attributes,
    }
}

/// Create JournalSpanData from span information.
///
/// Helper function to convert span data into journal format.
pub fn create_journal_span_data(
    trace_id: &str,
    span_id: &str,
    parent_span_id: Option<&str>,
    name: &str,
    kind: &str,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    status_code: &str,
    status_description: Option<&str>,
    attributes: Option<serde_json::Value>,
) -> JournalSpanData {
    JournalSpanData {
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: parent_span_id.map(|s| s.to_string()),
        name: name.to_string(),
        kind: kind.to_string(),
        start_time_unix_nano,
        end_time_unix_nano,
        status: JournalSpanStatus {
            code: status_code.to_string(),
            description: status_description.map(|s| s.to_string()),
        },
        attributes,
        events: vec![],
        links: vec![],
        resource: None,
    }
}

// ============================================================================
// Legacy exports for backwards compatibility (can be removed later)
// ============================================================================

/// Configuration for journal-based real-time streaming.
#[derive(Debug, Clone)]
pub struct JournalExporterConfig {
    /// Worker Coordinator gRPC endpoint
    pub endpoint: String,
    /// Whether to enable journal exporting (default: true if AGNT5_JOURNAL_STREAMING=true)
    pub enabled: bool,
}

impl Default for JournalExporterConfig {
    fn default() -> Self {
        let enabled = std::env::var("AGNT5_JOURNAL_STREAMING")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let endpoint = std::env::var("AGNT5_COORDINATOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:34186".to_string());

        Self { endpoint, enabled }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_exporter_config_default() {
        // By default, journal streaming should be disabled
        let config = JournalExporterConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "http://localhost:34186");
    }

    #[test]
    fn test_create_journal_span_data() {
        let span_data = create_journal_span_data(
            "trace123",
            "span456",
            Some("parent789"),
            "test_span",
            "internal",
            1000000000,
            2000000000,
            "ok",
            None,
            None,
        );
        assert_eq!(span_data.trace_id, "trace123");
        assert_eq!(span_data.span_id, "span456");
        assert_eq!(span_data.parent_span_id, Some("parent789".to_string()));
        assert_eq!(span_data.name, "test_span");
    }
}
