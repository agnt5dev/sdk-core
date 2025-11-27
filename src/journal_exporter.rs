// Journal exporter for real-time SSE streaming of spans and logs
//
// This module provides exporters that send OpenTelemetry spans and logs directly
// to the AGNT5 journal, bypassing the OTEL Collector's 5-second batch delay.
// This enables real-time SSE streaming for interactive scenarios like watching
// agent execution live in the UI.

// Note: Result and SdkError are available if needed for future extensions
use crate::pb::{
    worker_coordinator_service_client::WorkerCoordinatorServiceClient,
    WriteJournalEventRequest, WriteJournalEventsBatchRequest,
};
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, error, warn};

/// Journal-compatible span event data structure.
/// This mirrors the SpanEventData type from platform/pkg/journal/types.go
#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
pub struct JournalSpanStatus {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JournalSpanEvent {
    pub name: String,
    pub timestamp_unix_nano: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JournalSpanLink {
    pub trace_id: String,
    pub span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

/// JournalSpanExporter sends spans directly to the AGNT5 journal for real-time streaming.
///
/// This exporter bypasses the OTEL Collector's batch processing to enable
/// immediate SSE delivery of spans to the UI.
#[derive(Debug)]
pub struct JournalSpanExporter {
    client: Arc<Mutex<Option<WorkerCoordinatorServiceClient<Channel>>>>,
    endpoint: String,
    run_id: Arc<Mutex<Option<String>>>,
    tenant_id: Arc<Mutex<Option<String>>>,
}

impl JournalSpanExporter {
    /// Create a new journal span exporter.
    ///
    /// The exporter will lazily connect to the coordinator when the first span is exported.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The Worker Coordinator gRPC endpoint (e.g., "http://localhost:34186")
    pub fn new(endpoint: String) -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            endpoint,
            run_id: Arc::new(Mutex::new(None)),
            tenant_id: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the current run ID for span export.
    ///
    /// Spans will be associated with this run ID in the journal.
    pub async fn set_run_id(&self, run_id: Option<String>) {
        let mut guard = self.run_id.lock().await;
        *guard = run_id;
    }

    /// Set the tenant ID for span export.
    pub async fn set_tenant_id(&self, tenant_id: Option<String>) {
        let mut guard = self.tenant_id.lock().await;
        *guard = tenant_id;
    }

    /// Convert OpenTelemetry SpanData to journal format.
    fn convert_span_data(span: &SpanData) -> JournalSpanData {
        use opentelemetry::trace::Status;

        // Convert span kind to string
        let kind = match span.span_kind {
            opentelemetry::trace::SpanKind::Client => "client",
            opentelemetry::trace::SpanKind::Server => "server",
            opentelemetry::trace::SpanKind::Producer => "producer",
            opentelemetry::trace::SpanKind::Consumer => "consumer",
            opentelemetry::trace::SpanKind::Internal => "internal",
        };

        // Convert status
        let (status_code, status_desc) = match &span.status {
            Status::Unset => ("unset", None),
            Status::Ok => ("ok", None),
            Status::Error { description } => ("error", Some(description.to_string())),
        };

        // Convert attributes to JSON
        let attributes: Option<serde_json::Value> = if span.attributes.is_empty() {
            None
        } else {
            let attrs: serde_json::Map<String, serde_json::Value> = span
                .attributes
                .iter()
                .map(|kv| {
                    let value = match &kv.value {
                        opentelemetry::Value::Bool(b) => serde_json::Value::Bool(*b),
                        opentelemetry::Value::I64(i) => serde_json::Value::Number((*i).into()),
                        opentelemetry::Value::F64(f) => {
                            serde_json::Number::from_f64(*f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        }
                        opentelemetry::Value::String(s) => {
                            serde_json::Value::String(s.to_string())
                        }
                        opentelemetry::Value::Array(arr) => {
                            // Convert array values
                            let arr_values: Vec<serde_json::Value> = match arr {
                                opentelemetry::Array::Bool(vals) => {
                                    vals.iter().map(|v| serde_json::Value::Bool(*v)).collect()
                                }
                                opentelemetry::Array::I64(vals) => {
                                    vals.iter().map(|v| serde_json::Value::Number((*v).into())).collect()
                                }
                                opentelemetry::Array::F64(vals) => {
                                    vals.iter()
                                        .filter_map(|v| serde_json::Number::from_f64(*v))
                                        .map(serde_json::Value::Number)
                                        .collect()
                                }
                                opentelemetry::Array::String(vals) => {
                                    vals.iter()
                                        .map(|v| serde_json::Value::String(v.to_string()))
                                        .collect()
                                }
                                _ => vec![serde_json::Value::String("unknown".to_string())],
                            };
                            serde_json::Value::Array(arr_values)
                        }
                        _ => serde_json::Value::String(format!("{:?}", kv.value)),
                    };
                    (kv.key.to_string(), value)
                })
                .collect();
            Some(serde_json::Value::Object(attrs))
        };

        // Convert events
        let events: Vec<JournalSpanEvent> = span
            .events
            .iter()
            .map(|event| {
                let event_attrs: Option<serde_json::Value> = if event.attributes.is_empty() {
                    None
                } else {
                    let attrs: serde_json::Map<String, serde_json::Value> = event
                        .attributes
                        .iter()
                        .map(|kv| {
                            (
                                kv.key.to_string(),
                                serde_json::Value::String(format!("{:?}", kv.value)),
                            )
                        })
                        .collect();
                    Some(serde_json::Value::Object(attrs))
                };

                JournalSpanEvent {
                    name: event.name.to_string(),
                    timestamp_unix_nano: event.timestamp.duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    attributes: event_attrs,
                }
            })
            .collect();

        // Convert links
        let links: Vec<JournalSpanLink> = span
            .links
            .iter()
            .map(|link| {
                let link_attrs: Option<serde_json::Value> = if link.attributes.is_empty() {
                    None
                } else {
                    let attrs: serde_json::Map<String, serde_json::Value> = link
                        .attributes
                        .iter()
                        .map(|kv| {
                            (
                                kv.key.to_string(),
                                serde_json::Value::String(format!("{:?}", kv.value)),
                            )
                        })
                        .collect();
                    Some(serde_json::Value::Object(attrs))
                };

                JournalSpanLink {
                    trace_id: span.span_context.trace_id().to_string(),
                    span_id: link.span_context.span_id().to_string(),
                    attributes: link_attrs,
                }
            })
            .collect();

        // Get parent span ID if present (check if it's not the invalid/zero span ID)
        let parent_span_id = {
            let parent_str = span.parent_span_id.to_string();
            // SpanId::INVALID is all zeros - "0000000000000000"
            if parent_str == "0000000000000000" || parent_str.is_empty() {
                None
            } else {
                Some(parent_str)
            }
        };

        JournalSpanData {
            trace_id: span.span_context.trace_id().to_string(),
            span_id: span.span_context.span_id().to_string(),
            parent_span_id,
            name: span.name.to_string(),
            kind: kind.to_string(),
            start_time_unix_nano: span.start_time.duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
            end_time_unix_nano: span.end_time.duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
            status: JournalSpanStatus {
                code: status_code.to_string(),
                description: status_desc,
            },
            attributes,
            events,
            links,
            resource: None, // TODO: Add resource attributes if needed
        }
    }

    /// Extract run_id from span attributes.
    fn extract_run_id(span: &SpanData) -> Option<String> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == "run.id")
            .map(|kv| match &kv.value {
                opentelemetry::Value::String(s) => s.to_string(),
                _ => format!("{:?}", kv.value),
            })
    }

    /// Check if span has is_streaming=true attribute.
    /// Only spans with this attribute should be exported to the journal for real-time SSE.
    fn is_streaming_span(span: &SpanData) -> bool {
        span.attributes
            .iter()
            .any(|kv| {
                kv.key.as_str() == "agnt5.is_streaming" && matches!(&kv.value, opentelemetry::Value::Bool(true))
            })
    }
}

impl SpanExporter for JournalSpanExporter {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = std::result::Result<(), OTelSdkError>> + Send {
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        let default_run_id = self.run_id.clone();
        let default_tenant_id = self.tenant_id.clone();

        async move {
            if batch.is_empty() {
                return Ok(());
            }

            // Ensure connected
            {
                let mut client_guard = client.lock().await;
                if client_guard.is_none() {
                    debug!("Connecting journal exporter to {}", endpoint);
                    match Channel::from_shared(endpoint.clone()) {
                        Ok(channel_builder) => {
                            match channel_builder.connect().await {
                                Ok(channel) => {
                                    *client_guard = Some(WorkerCoordinatorServiceClient::new(channel));
                                }
                                Err(e) => {
                                    warn!("Failed to connect journal exporter: {}", e);
                                    return Ok(()); // Don't fail the export, just skip
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Invalid journal exporter endpoint: {}", e);
                            return Ok(());
                        }
                    }
                }
            }

            // Get default run_id and tenant_id
            let default_run = default_run_id.lock().await.clone();
            let default_tenant = default_tenant_id.lock().await.clone();

            // Build batch of requests
            let requests: Vec<WriteJournalEventRequest> = batch
                .iter()
                .filter_map(|span| {
                    // Only export spans that have is_streaming=true attribute
                    // This ensures journal export only happens for streaming calls
                    if !Self::is_streaming_span(span) {
                        debug!("Skipping span {} - not a streaming span", span.name);
                        return None;
                    }

                    // Try to get run_id from span attributes, fall back to default
                    let run_id = Self::extract_run_id(span).or_else(|| default_run.clone());

                    let run_id = match run_id {
                        Some(id) => id,
                        None => {
                            // Skip spans without run_id - they can't be associated with a journal stream
                            debug!("Skipping span {} - no run_id", span.name);
                            return None;
                        }
                    };

                    // Convert span to journal format
                    let span_data = Self::convert_span_data(span);
                    let data = match serde_json::to_vec(&span_data) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!("Failed to serialize span data: {}", e);
                            return None;
                        }
                    };

                    Some(WriteJournalEventRequest {
                        run_id,
                        event_type: "span".to_string(),
                        data,
                        trace_id: span_data.trace_id,
                        span_id: span_data.span_id,
                        tenant_id: default_tenant.clone().unwrap_or_default(),
                    })
                })
                .collect();

            if requests.is_empty() {
                return Ok(());
            }

            // Send batch
            let mut client_guard = client.lock().await;
            if let Some(ref mut grpc_client) = *client_guard {
                let request = WriteJournalEventsBatchRequest { events: requests };
                match grpc_client.write_journal_events_batch(request).await {
                    Ok(response) => {
                        let inner = response.into_inner();
                        if !inner.errors.is_empty() {
                            warn!(
                                "Journal batch export had {} errors, {} written",
                                inner.errors.len(),
                                inner.written_count
                            );
                        } else {
                            debug!("Exported {} spans to journal", inner.written_count);
                        }
                    }
                    Err(e) => {
                        error!("Failed to export spans to journal: {}", e);
                        // Don't return error - just log it. We don't want to fail the exporter.
                    }
                }
            }

            Ok(())
        }
    }

    fn shutdown(&mut self) -> std::result::Result<(), OTelSdkError> {
        // Nothing special needed for shutdown
        Ok(())
    }
}

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

/// Create a journal span exporter if enabled by environment variable.
///
/// Returns None if journal streaming is disabled (AGNT5_JOURNAL_STREAMING != "true").
pub fn create_journal_exporter() -> Option<JournalSpanExporter> {
    let config = JournalExporterConfig::default();
    if config.enabled {
        Some(JournalSpanExporter::new(config.endpoint))
    } else {
        None
    }
}

/// Create a journal span exporter unconditionally.
///
/// This is used when span filtering is done at export time based on agnt5.is_streaming attribute.
/// Always returns Some() unless the endpoint cannot be determined.
pub fn create_journal_exporter_always() -> Option<JournalSpanExporter> {
    let endpoint = std::env::var("AGNT5_COORDINATOR_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:34186".to_string());
    Some(JournalSpanExporter::new(endpoint))
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
    fn test_create_journal_exporter_disabled() {
        // Should return None when not enabled
        let exporter = create_journal_exporter();
        assert!(exporter.is_none());
    }
}
