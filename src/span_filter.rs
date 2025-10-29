/// Span filtering exporter to remove internal h2/HTTP2 spans from traces
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::error::OTelSdkError;
use std::fmt;

/// FilteringSpanExporter wraps another span exporter and filters out h2/HTTP2 internal spans
#[derive(Debug)]
pub struct FilteringSpanExporter<E: SpanExporter> {
    inner: E,
}

impl<E: SpanExporter> FilteringSpanExporter<E> {
    /// Create a new filtering span exporter that wraps another exporter
    pub fn new(inner: E) -> Self {
        Self { inner }
    }

    /// Check if a span should be filtered based on its name
    fn should_filter_span(span_name: &str) -> bool {
        let name_lower = span_name.to_lowercase();

        // Filter patterns for h2/HTTP2 internal operations
        let filter_patterns = [
            // Generic h2 patterns
            ".h2",
            "recv.h2",
            "send.h2",
            "grpc.io/server/bidi_stream",
            "grpc.io/client/bidi_stream",
            "http2.framer",
            "transport: http2",
            // Specific HTTP/2 frame operations (observed in production)
            "try_reclaim_frame",
            "pop_frame",
            "framedwrite",
            "framedread",
            "popped",
            "stream flow",
            "connection flow",
            "poll_ready",
            "poll_next",
            "poll",                          // Generic poll operations
            "reserve_capacity",
            "try_assign_capacity",
            "prioritize::queue_frame",
            "assign_connection_capacity",
            "send_data",
            "hpack::",
            "recv_stream_window_update",
            "updating stream flow",
            "updating connection flow",
            "decode_frame",
        ];

        filter_patterns.iter().any(|pattern| name_lower.contains(pattern))
    }
}

impl<E: SpanExporter + fmt::Debug> SpanExporter for FilteringSpanExporter<E> {
    fn export(&self, batch: Vec<SpanData>) -> impl std::future::Future<Output = Result<(), OTelSdkError>> + Send {
        // Filter out h2 spans before exporting
        let filtered_batch: Vec<SpanData> = batch
            .into_iter()
            .filter(|span| !Self::should_filter_span(&span.name))
            .collect();

        // Forward filtered batch to inner exporter
        self.inner.export(filtered_batch)
    }

    fn shutdown(&mut self) -> Result<(), OTelSdkError> {
        self.inner.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_filter_h2_spans() {
        assert!(FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span("try_reclaim_frame"));
        assert!(FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span("FramedWrite::flush"));
        assert!(FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span("updating stream flow"));
        assert!(FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span("hpack::decode"));
    }

    #[test]
    fn test_should_not_filter_user_spans() {
        assert!(!FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span("function.greet_user"));
        assert!(!FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span("GET /api/users"));
        assert!(!FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span("workflow.process_data"));
    }
}
