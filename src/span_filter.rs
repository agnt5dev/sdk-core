use opentelemetry_sdk::error::OTelSdkError;
/// Span filtering exporter to remove internal h2/HTTP2 spans from traces
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::Resource;
use std::fmt;
use std::time::Duration;

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
            "poll", // Generic poll operations
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

        filter_patterns
            .iter()
            .any(|pattern| name_lower.contains(pattern))
    }
}

impl<E: SpanExporter + fmt::Debug> SpanExporter for FilteringSpanExporter<E> {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = Result<(), OTelSdkError>> + Send {
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

    fn shutdown_with_timeout(&mut self, timeout: Duration) -> Result<(), OTelSdkError> {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&mut self) -> Result<(), OTelSdkError> {
        self.inner.force_flush()
    }

    // The trait default is a no-op, so without this the inner OTLP exporter
    // keeps an empty resource and spans export without service.name (AGNT5-1388).
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_filter_h2_spans() {
        assert!(
            FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span(
                "try_reclaim_frame"
            )
        );
        assert!(
            FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span(
                "FramedWrite::flush"
            )
        );
        assert!(
            FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span(
                "updating stream flow"
            )
        );
        assert!(
            FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span(
                "hpack::decode"
            )
        );
    }

    #[derive(Debug, Clone, Default)]
    struct ResourceRecorder {
        resource: std::sync::Arc<std::sync::Mutex<Option<Resource>>>,
    }

    impl SpanExporter for ResourceRecorder {
        async fn export(&self, _batch: Vec<SpanData>) -> Result<(), OTelSdkError> {
            Ok(())
        }

        fn set_resource(&mut self, resource: &Resource) {
            *self.resource.lock().unwrap() = Some(resource.clone());
        }
    }

    #[test]
    fn test_provider_resource_reaches_inner_exporter() {
        let recorder = ResourceRecorder::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name("agnt5-worker")
                    .build(),
            )
            .with_simple_exporter(FilteringSpanExporter::new(recorder.clone()))
            .build();

        let resource = recorder
            .resource
            .lock()
            .unwrap()
            .clone()
            .expect("inner exporter never received the provider resource");
        assert_eq!(
            resource
                .get(&opentelemetry::Key::new("service.name"))
                .map(|v| v.to_string()),
            Some("agnt5-worker".to_string())
        );
        let _ = provider.shutdown();
    }

    #[test]
    fn test_should_not_filter_user_spans() {
        assert!(
            !FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span(
                "function.greet_user"
            )
        );
        assert!(
            !FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span(
                "GET /api/users"
            )
        );
        assert!(
            !FilteringSpanExporter::<opentelemetry_otlp::SpanExporter>::should_filter_span(
                "workflow.process_data"
            )
        );
    }
}
