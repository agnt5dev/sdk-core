// OpenTelemetry telemetry module with OTLP exporter for traces and logs
use crate::error::SdkError;
use opentelemetry::global::BoxedSpan;
use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{Span, SpanKind, Status, Tracer, TracerProvider};
use opentelemetry::{baggage::BaggageExt, global, Context, KeyValue};
use opentelemetry_otlp::{LogExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    propagation::{BaggagePropagator, TraceContextPropagator},
    trace::SdkTracerProvider,
    Resource,
};
use std::collections::HashMap;
use tracing_subscriber::{fmt::format::Writer, layer::SubscriberExt, EnvFilter, Registry};

/// Custom field formatter that prioritizes invocation.id in log output
struct InvocationFieldFormatter;

impl<'writer> tracing_subscriber::fmt::format::FormatFields<'writer> for InvocationFieldFormatter {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        writer: Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        use tracing::field::{Field, Visit};

        struct FieldVisitor<'a> {
            writer: Writer<'a>,
            result: std::fmt::Result,
            invocation_id: Option<String>,
            other_fields: Vec<(String, String)>,
        }

        impl<'a> Visit for FieldVisitor<'a> {
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "invocation.id" {
                    self.invocation_id = Some(value.to_string());
                } else {
                    self.other_fields
                        .push((field.name().to_string(), value.to_string()));
                }
            }

            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                let formatted = format!("{:?}", value);
                if field.name() == "invocation.id" {
                    self.invocation_id = Some(formatted);
                } else {
                    self.other_fields
                        .push((field.name().to_string(), formatted));
                }
            }
        }

        let mut visitor = FieldVisitor {
            writer,
            result: Ok(()),
            invocation_id: None,
            other_fields: Vec::new(),
        };

        fields.record(&mut visitor);

        // Write invocation.id first if present
        if let Some(inv_id) = visitor.invocation_id {
            write!(visitor.writer, "invocation.id={} ", inv_id)?;
        }

        // Write other fields
        for (name, value) in visitor.other_fields {
            if !name.is_empty() {
                write!(visitor.writer, "{}={} ", name, value)?;
            }
        }

        visitor.result
    }
}

/// Initialize OpenTelemetry with OTLP exporter and structured logging
pub fn init_telemetry(service_name: &str, service_version: &str) -> Result<(), SdkError> {
    let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    tracing::info!(
        "Initializing OpenTelemetry with OTLP endpoint: {}",
        otel_endpoint
    );

    // Create resource with service information
    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .with_attributes(vec![KeyValue::new(
            "service.version",
            service_version.to_string(),
        )])
        .build();

    // Build OTLP exporters for traces and logs
    let trace_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint.clone())
        .build()
        .map_err(|e| {
            SdkError::Other(anyhow::anyhow!(
                "Failed to create OTLP trace exporter: {}",
                e
            ))
        })?;

    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to create OTLP log exporter: {}", e))
        })?;

    // Create tracer provider with batch exporter
    let trace_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(trace_exporter)
        .build();

    // Create logger provider with batch exporter
    let log_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();

    // Get tracer from provider before setting as global
    let tracer = trace_provider.tracer("agnt5-sdk-core");

    // Set as global tracer provider
    global::set_tracer_provider(trace_provider);

    // Set up composite propagation for both trace context and baggage
    let trace_propagator = TraceContextPropagator::new();
    let baggage_propagator = BaggagePropagator::new();

    // Create composite propagator that handles both trace context and baggage
    let composite_propagator = TextMapCompositePropagator::new(vec![
        Box::new(trace_propagator) as Box<dyn TextMapPropagator + Send + Sync>,
        Box::new(baggage_propagator) as Box<dyn TextMapPropagator + Send + Sync>,
    ]);

    global::set_text_map_propagator(composite_propagator);

    // Set up tracing subscriber with OpenTelemetry layers
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Set up OpenTelemetry logs appender
    let log_appender =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&log_provider);

    // Create subscriber with filtered console output and OpenTelemetry
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // Default filter: info level for our code, warn for dependencies
        EnvFilter::new("agnt5=info,h2=warn,hyper=warn,tonic=warn,tower=warn,info")
    });

    // Create custom fmt layer that includes invocation.id in log output
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(true)
        .with_line_number(true)
        .fmt_fields(InvocationFieldFormatter);

    let subscriber = Registry::default()
        .with(telemetry_layer)
        .with(log_appender)
        .with(fmt_layer)
        .with(env_filter);

    // Set as global default subscriber
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to set tracing subscriber: {}", e)))?;

    tracing::info!(
        "OpenTelemetry with tracing integration initialized successfully for service: {}",
        service_name
    );
    Ok(())
}

/// Extract trace context and baggage from runtime message metadata
pub fn extract_context_from_runtime_message(metadata: &HashMap<String, String>) -> Context {
    tracing::debug!("Extracting context from metadata: {:?}", metadata);

    // Extract trace context first
    let trace_propagator = TraceContextPropagator::new();
    let ctx = trace_propagator.extract(metadata);

    // Then extract baggage using the trace context
    let baggage_propagator = BaggagePropagator::new();
    let final_ctx = baggage_propagator.extract_with_context(&ctx, metadata);

    // Debug log baggage contents
    let baggage = final_ctx.baggage();
    tracing::debug!(
        "Extracted baggage items: {:?}",
        baggage.iter().collect::<Vec<_>>()
    );

    final_ctx
}

/// Create a span for function execution with proper parent context
pub fn create_function_span(
    function_name: &str,
    service_name: &str,
    worker_id: &str,
    invocation_id: &str,
    parent_context: Option<Context>,
    metadata: Option<&HashMap<String, String>>,
) -> BoxedSpan {
    let tracer = global::tracer("agnt5-sdk-core");

    let mut attributes = vec![
        KeyValue::new("function.name", function_name.to_string()),
        KeyValue::new("service.name", service_name.to_string()),
        KeyValue::new("worker.id", worker_id.to_string()),
        KeyValue::new("invocation.id", invocation_id.to_string()),
    ];

    // Extract baggage items as span attributes if parent context exists
    if let Some(ref ctx) = parent_context {
        let baggage = ctx.baggage();
        let baggage_items: Vec<_> = baggage.iter().collect();
        tracing::debug!(
            "Adding {} baggage items as span attributes: {:?}",
            baggage_items.len(),
            baggage_items
        );

        for (key, (value, _metadata)) in baggage.iter() {
            // Add baggage items with "baggage." prefix to distinguish them
            let attr_key = format!("baggage.{}", key);
            attributes.push(KeyValue::new(attr_key.clone(), value.to_string()));
            tracing::debug!("Added baggage attribute: {} = {}", attr_key, value);
        }
    } else {
        tracing::debug!("No parent context provided for baggage extraction");
    }

    if let Some(meta) = metadata {
        if let Some(tenant_id) = meta.get("tenant_id") {
            attributes.push(KeyValue::new("tenant.id", tenant_id.clone()));
        }
        if let Some(run_id) = meta.get("run_id") {
            attributes.push(KeyValue::new("run.id", run_id.clone()));
        }
        if let Some(step_name) = meta.get("step_name") {
            attributes.push(KeyValue::new("function.step_name", step_name.clone()));
        }
        if let Some(attempt) = meta
            .get("attempt")
            .or_else(|| meta.get("attempt_number"))
            .or_else(|| meta.get("step_attempt"))
        {
            if let Ok(parsed) = attempt.parse::<i64>() {
                attributes.push(KeyValue::new("function.step_attempt", parsed));
            }
        }
        if let Some(traceparent) = meta.get("traceparent") {
            attributes.push(KeyValue::new("traceparent", traceparent.clone()));
        }
        if let Some(user_id) = meta.get("user_id") {
            attributes.push(KeyValue::new("user.id", user_id.clone()));
        }
        if let Some(request_id) = meta.get("request_id") {
            attributes.push(KeyValue::new("request.id", request_id.clone()));
        }
    }

    let span_builder = tracer
        .span_builder(format!("function.{}", function_name))
        .with_kind(SpanKind::Server)
        .with_attributes(attributes);

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

/// End a span (helper function since Span trait may not be accessible)
pub fn end_span(mut span: BoxedSpan) {
    span.end();
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
