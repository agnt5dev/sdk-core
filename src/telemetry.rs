// OpenTelemetry telemetry module with OTLP exporter for traces and logs
use crate::error::SdkError;
use opentelemetry::global::BoxedSpan;
use opentelemetry::propagation::{Extractor, TextMapCompositePropagator, TextMapPropagator};
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
use tracing_subscriber::{fmt::format::Writer, layer::SubscriberExt, Layer as _, EnvFilter, Registry};

// Newtype wrapper to implement Extractor trait for HashMap (avoids orphan rule)
struct HashMapExtractor<'a>(&'a HashMap<String, String>);

impl<'a> Extractor for HashMapExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }
}

/// Custom field formatter that prioritizes run.id in log output
struct RunFieldFormatter;

impl<'writer> tracing_subscriber::fmt::format::FormatFields<'writer> for RunFieldFormatter {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        writer: Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        use tracing::field::{Field, Visit};

        struct FieldVisitor<'a> {
            writer: Writer<'a>,
            result: std::fmt::Result,
            run_id: Option<String>,
            other_fields: Vec<(String, String)>,
        }

        impl<'a> Visit for FieldVisitor<'a> {
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "run.id" {
                    self.run_id = Some(value.to_string());
                } else {
                    self.other_fields
                        .push((field.name().to_string(), value.to_string()));
                }
            }

            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                let formatted = format!("{:?}", value);
                if field.name() == "run.id" {
                    self.run_id = Some(formatted);
                } else {
                    self.other_fields
                        .push((field.name().to_string(), formatted));
                }
            }
        }

        let mut visitor = FieldVisitor {
            writer,
            result: Ok(()),
            run_id: None,
            other_fields: Vec::new(),
        };

        fields.record(&mut visitor);

        // Write run.id first if present
        if let Some(run_id) = visitor.run_id {
            write!(visitor.writer, "run.id={} ", run_id)?;
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

    // Print to stderr immediately so it's visible even if tracing setup fails
    eprintln!("🔭 AGNT5 OpenTelemetry Configuration:");
    eprintln!("   Endpoint: {}", otel_endpoint);
    eprintln!("   Service:  {}", service_name);
    eprintln!("   Version:  {}", service_version);

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

    // Create filters for both console and OpenTelemetry (need separate instances)
    let filter_directive = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        // Default filter: debug level for AGNT5 and Python SDK, error for noisy dependencies
        // Filter out noisy gRPC/HTTP2 traces from h2, hyper, tonic, tower
        // Include python_log for Python SDK ctx.logger logs
        "agnt5=debug,python_log=debug,h2=error,hyper=error,tonic=warn,tower=warn".to_string()
    });

    let console_filter = EnvFilter::new(&filter_directive);
    let otel_filter = EnvFilter::new(&filter_directive);

    // Create custom fmt layer that includes run.id in log output
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(true)
        .with_line_number(true)
        .fmt_fields(RunFieldFormatter)
        .with_filter(console_filter);  // Filter console output

    let subscriber = Registry::default()
        .with(telemetry_layer)
        .with(log_appender.with_filter(otel_filter))  // Filter OTLP logs too!
        .with(fmt_layer);  // Filtered console output

    // Set as global default subscriber
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to set tracing subscriber: {}", e)))?;

    // Initialize LogTracer to bridge log crate to tracing
    // This allows log::info!() calls to be forwarded to tracing (and thus OpenTelemetry)
    if let Err(e) = tracing_log::LogTracer::init() {
        eprintln!("⚠️  Warning: Failed to initialize LogTracer (log → tracing bridge): {}", e);
        eprintln!("   Some logs using log::info!() may not appear in OpenTelemetry");
    }

    tracing::info!(
        "OpenTelemetry with tracing integration initialized successfully for service: {}",
        service_name
    );
    Ok(())
}

/// Extract trace context and baggage from runtime message metadata
pub fn extract_context_from_runtime_message(metadata: &HashMap<String, String>) -> Context {
    // Wrap HashMap in our Extractor implementation
    let extractor = HashMapExtractor(metadata);

    // Extract trace context first
    let trace_propagator = TraceContextPropagator::new();
    let ctx = trace_propagator.extract(&extractor);

    // Check if we extracted valid trace context and log warning if not
    use opentelemetry::trace::TraceContextExt;
    let span = ctx.span();
    let span_context = span.span_context();
    if !span_context.is_valid() {
        tracing::warn!("⚠️  Failed to extract valid trace context from metadata - will create new root trace");
    }

    // Then extract baggage using the trace context
    let baggage_propagator = BaggagePropagator::new();
    let final_ctx = baggage_propagator.extract_with_context(&ctx, &extractor);

    final_ctx
}

/// Create a span for function execution with proper parent context
pub fn create_function_span(
    function_name: &str,
    service_name: &str,
    worker_id: &str,
    run_id: &str,
    parent_context: Option<Context>,
    metadata: Option<&HashMap<String, String>>,
) -> BoxedSpan {
    // Default to "function" for backwards compatibility
    create_component_span(
        function_name,
        "function",
        service_name,
        worker_id,
        run_id,
        parent_context,
        metadata,
    )
}

/// Create a span for any component type (function, workflow, agent, tool, entity)
pub fn create_component_span(
    component_name: &str,
    component_type: &str,
    service_name: &str,
    worker_id: &str,
    run_id: &str,
    parent_context: Option<Context>,
    metadata: Option<&HashMap<String, String>>,
) -> BoxedSpan {
    let tracer = global::tracer("agnt5-sdk-core");

    let mut attributes = vec![
        KeyValue::new("component.name", component_name.to_string()),
        KeyValue::new("component.type", component_type.to_string()),
        KeyValue::new("service.name", service_name.to_string()),
        KeyValue::new("worker.id", worker_id.to_string()),
        KeyValue::new("run.id", run_id.to_string()),
    ];

    // Extract baggage items as span attributes if parent context exists
    if let Some(ref ctx) = parent_context {
        let baggage = ctx.baggage();
        for (key, (value, _metadata)) in baggage.iter() {
            // Add baggage items with "baggage." prefix to distinguish them
            let attr_key = format!("baggage.{}", key);
            attributes.push(KeyValue::new(attr_key.clone(), value.to_string()));
        }
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
        .span_builder(format!("{}.{}", component_type, component_name))
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

/// Create a span for tool execution following OpenTelemetry Gen AI semantic conventions
///
/// Per the spec, tool execution spans should use:
/// - Span name: `execute_tool {tool_name}`
/// - gen_ai.operation.name: "execute_tool"
/// - gen_ai.tool.name: The tool name
/// - gen_ai.tool.call.id: Unique identifier for this tool call (optional)
/// - gen_ai.tool.description: Tool description (optional)
pub fn create_tool_execution_span(
    tool_name: &str,
    tool_call_id: Option<&str>,
    tool_description: Option<&str>,
    arguments: Option<&str>,
) -> BoxedSpan {
    let tracer = global::tracer("agnt5-sdk-core");

    // Span name format: "execute_tool {tool_name}" per OpenTelemetry spec
    let span_name = format!("execute_tool {}", tool_name);

    let mut attributes = vec![
        // Required attributes per OpenTelemetry Gen AI conventions
        KeyValue::new("gen_ai.operation.name", "execute_tool"),
        KeyValue::new("gen_ai.tool.name", tool_name.to_string()),
    ];

    // Optional attributes
    if let Some(call_id) = tool_call_id {
        attributes.push(KeyValue::new("gen_ai.tool.call.id", call_id.to_string()));
    }

    if let Some(description) = tool_description {
        attributes.push(KeyValue::new("gen_ai.tool.description", description.to_string()));
    }

    // Capture tool arguments if provided (typically JSON string)
    if let Some(args) = arguments {
        // Truncate to prevent huge span attributes
        let truncated = if args.len() > 2000 {
            format!("{}... [truncated {} bytes]", &args[..2000], args.len() - 2000)
        } else {
            args.to_string()
        };
        attributes.push(KeyValue::new("gen_ai.tool.arguments", truncated));
    }

    // Tool execution is INTERNAL span kind (not CLIENT)
    let span = tracer
        .span_builder(span_name)
        .with_kind(SpanKind::Internal)
        .with_attributes(attributes)
        .start(&tracer);

    span
}

/// Record tool execution success with result
pub fn record_tool_success(span: &mut BoxedSpan, result: Option<&str>) {
    span.set_attribute(KeyValue::new("gen_ai.tool.status", "success"));

    if let Some(res) = result {
        // Truncate result to prevent huge span attributes
        let truncated = if res.len() > 5000 {
            format!("{}... [truncated {} bytes]", &res[..5000], res.len() - 5000)
        } else {
            res.to_string()
        };
        span.set_attribute(KeyValue::new("gen_ai.tool.result", truncated));
    }

    span.set_status(Status::Ok);
}

/// Record tool execution error
pub fn record_tool_error(span: &mut BoxedSpan, error_msg: &str) {
    span.set_attribute(KeyValue::new("gen_ai.tool.status", "error"));
    span.set_attribute(KeyValue::new("gen_ai.tool.error", error_msg.to_string()));
    span.set_status(Status::error(error_msg.to_string()));
}

/// End a span (helper function since Span trait may not be accessible)
pub fn end_span(mut span: BoxedSpan) {
    span.end();
}

/// Force flush all pending telemetry data (spans and logs)
///
/// This should be called before worker shutdown to ensure batched spans are exported.
/// The batch span processor buffers spans with a 5-second timeout by default.
pub fn flush_telemetry() -> Result<(), SdkError> {
    // The global tracer provider doesn't expose force_flush directly
    // We need to access it through the TracerProvider trait
    // For now, use a simple timeout to allow batch processor to flush
    // Using 2 seconds to ensure batch has time to export
    use std::time::Duration;
    std::thread::sleep(Duration::from_secs(2));

    Ok(())
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
