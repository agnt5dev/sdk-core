// OpenTelemetry telemetry module with OTLP exporter
use std::collections::HashMap;
use opentelemetry::{global, KeyValue, Context};
use opentelemetry::trace::{Span, SpanKind, Tracer, Status};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::global::BoxedSpan;
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource, propagation::TraceContextPropagator};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use crate::error::SdkError;

/// Initialize OpenTelemetry with OTLP exporter (should be called from async context)
pub fn init_telemetry(service_name: &str, service_version: &str) -> Result<(), SdkError> {
    let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());
    
    tracing::info!("Initializing OpenTelemetry with OTLP endpoint: {}", otel_endpoint);

    // Create resource with service information
    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .with_attributes(vec![
            KeyValue::new("service.version", service_version.to_string()),
        ])
        .build();

    // Build OTLP exporter with gRPC
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to create OTLP exporter: {}", e)))?;

    // Create tracer provider with batch exporter
    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    // Set as global tracer provider
    global::set_tracer_provider(provider);
    
    // Set up trace context propagation
    global::set_text_map_propagator(TraceContextPropagator::new());
    
    tracing::info!("OpenTelemetry initialized successfully for service: {}", service_name);
    Ok(())
}

/// Extract trace context from runtime message metadata (e.g., traceparent header)
pub fn extract_trace_context_from_runtime_message(metadata: &HashMap<String, String>) -> Context {
    let propagator = TraceContextPropagator::new();
    propagator.extract(metadata)
}

/// Create a span for function execution with proper parent context
pub fn create_function_span(
    function_name: &str,
    service_name: &str,
    worker_id: &str,
    invocation_id: &str,
    parent_context: Option<Context>,
) -> BoxedSpan {
    let tracer = global::tracer("agnt5-sdk-core");
    
    let span_builder = tracer
        .span_builder(format!("function.{}", function_name))
        .with_kind(SpanKind::Server)
        .with_attributes(vec![
            KeyValue::new("function.name", function_name.to_string()),
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("worker.id", worker_id.to_string()),
            KeyValue::new("invocation.id", invocation_id.to_string()),
        ]);

    // Set parent context if provided
    let span = if let Some(parent_ctx) = parent_context {
        span_builder.start_with_context(&tracer, &parent_ctx)
    } else {
        span_builder.start(&tracer)
    };

    span
}

/// Record span success
pub fn record_span_success(span: &mut BoxedSpan, output_size: usize) {
    span.set_attribute(KeyValue::new("function.status", "success"));
    span.set_attribute(KeyValue::new("function.output_size", output_size as i64));
    span.set_status(Status::Ok);
}

/// Record span error
pub fn record_span_error(span: &mut BoxedSpan, error_msg: &str) {
    span.set_attribute(KeyValue::new("function.status", "error"));
    span.set_attribute(KeyValue::new("function.error", error_msg.to_string()));
    span.set_status(Status::error(error_msg.to_string()));
}

/// Stub shutdown function - implementation pending due to known issues
pub fn shutdown_telemetry() {
    // For now, just log that shutdown was called
    tracing::info!("Telemetry shutdown requested");
    // Note: global::shutdown_tracer_provider() has known hanging issues in v0.30
    // Will implement proper shutdown in later phase
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_init() {
        // Simple test to ensure init function works
        assert!(init_telemetry("test-service", "1.0.0").is_ok());
    }
}