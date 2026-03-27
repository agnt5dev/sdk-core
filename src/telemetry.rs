// OpenTelemetry telemetry module with OTLP exporter for traces, logs, and metrics
use crate::error::SdkError;
use opentelemetry::global::BoxedSpan;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::propagation::{Extractor, TextMapCompositePropagator, TextMapPropagator};
use opentelemetry::trace::{Span, SpanKind, Status, Tracer, TracerProvider};
use opentelemetry::{baggage::BaggageExt, global, Context, KeyValue};
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::SdkMeterProvider,
    propagation::{BaggagePropagator, TraceContextPropagator},
    trace::SdkTracerProvider,
    Resource,
};
use std::sync::OnceLock;
use std::collections::HashMap;
use tracing_subscriber::{fmt::format::Writer, layer::SubscriberExt, Layer as _, EnvFilter, Registry};

// Global storage for tenant_id and deployment_id (set at init time)
static TENANT_ID: OnceLock<Option<String>> = OnceLock::new();
static DEPLOYMENT_ID: OnceLock<Option<String>> = OnceLock::new();

/// Get the configured tenant_id (set from AGNT5_TENANT_ID env var)
pub fn get_tenant_id() -> Option<&'static str> {
    TENANT_ID.get().and_then(|opt| opt.as_deref())
}

/// Get the configured deployment_id (set from AGNT5_DEPLOYMENT_ID env var)
pub fn get_deployment_id() -> Option<&'static str> {
    DEPLOYMENT_ID.get().and_then(|opt| opt.as_deref())
}

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

    tracing::debug!(
        "Initializing OpenTelemetry: endpoint={}, service={}",
        otel_endpoint,
        service_name
    );

    // Extract deployment_id and tenant_id from environment variables
    // These are set by the control plane when deploying workers
    let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").ok();
    let tenant_id = std::env::var("AGNT5_TENANT_ID").ok();

    // Store globally for use in span/metric/log creation
    let _ = TENANT_ID.set(tenant_id.clone());
    let _ = DEPLOYMENT_ID.set(deployment_id.clone());

    // Create resource attributes
    let mut resource_attributes = vec![
        KeyValue::new("service.version", service_version.to_string()),
    ];

    if let Some(ref deployment_id) = deployment_id {
        resource_attributes.push(KeyValue::new("deployment.id", deployment_id.clone()));
    }

    if let Some(ref tenant_id) = tenant_id {
        resource_attributes.push(KeyValue::new("tenant.id", tenant_id.clone()));
    }

    // Create resource with service information and deployment/tenant IDs
    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .with_attributes(resource_attributes)
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
        .with_endpoint(otel_endpoint.clone())
        .build()
        .map_err(|e| {
            SdkError::Other(anyhow::anyhow!("Failed to create OTLP log exporter: {}", e))
        })?;

    // Wrap trace exporter with filtering to remove h2 spans
    let filtering_exporter = crate::span_filter::FilteringSpanExporter::new(trace_exporter);

    // Create tracer provider builder
    let trace_provider_builder = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(filtering_exporter);

    // NOTE: Real-time SSE streaming events go through the unified JournalEventQueue.
    // The flush task routes SSE-only events via EventStream and boundary events via
    // WriteJournalEventsBatch — both directly to EE, bypassing the dispatch stream.

    // Build tracer provider
    let trace_provider = trace_provider_builder.build();

    // Create logger provider with batch exporter
    let log_provider = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(log_exporter)
        .build();

    // Create metrics exporter and meter provider
    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint.clone())
        .build()
        .map_err(|e| {
            SdkError::Other(anyhow::anyhow!(
                "Failed to create OTLP metric exporter: {}",
                e
            ))
        })?;

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();

    // Set as global meter provider
    global::set_meter_provider(meter_provider);

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

    // Check AGNT5_DEBUG for debug mode
    let debug_enabled = std::env::var("AGNT5_DEBUG")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    // Console filter: controlled by RUST_LOG / AGNT5_DEBUG, keeps output clean in production
    let console_directive = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        if debug_enabled {
            "agnt5=debug,agnt5_sdk_python=debug,h2=error,hyper=error,tonic=warn,tower=warn".to_string()
        } else {
            "agnt5=warn,agnt5_sdk_python=warn,h2=error,hyper=error,tonic=error,tower=error".to_string()
        }
    });
    let console_filter = EnvFilter::new(&console_directive);

    // OTLP filter: user application logs (agnt5_sdk_python, agnt5_sdk_typescript) always
    // exported at all levels, so the control plane can query them by log_source="application" + run_id.
    // Platform-internal logs stay at warn. Override with AGNT5_OTEL_LOG_FILTER.
    let otel_directive = std::env::var("AGNT5_OTEL_LOG_FILTER").unwrap_or_else(|_| {
        if debug_enabled {
            "agnt5=debug,agnt5_sdk_python=trace,agnt5_sdk_typescript=trace,h2=error,hyper=error,tonic=warn,tower=warn".to_string()
        } else {
            "agnt5=warn,agnt5_sdk_python=trace,agnt5_sdk_typescript=trace,h2=error,hyper=error,tonic=error,tower=error".to_string()
        }
    });
    let otel_filter = EnvFilter::new(&otel_directive);

    // Create custom fmt layer with clean output (no file paths or line numbers)
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .fmt_fields(RunFieldFormatter)
        .with_filter(console_filter);

    let subscriber = Registry::default()
        .with(telemetry_layer)
        .with(log_appender.with_filter(otel_filter))  // OTLP logs: all user levels, platform at warn
        .with(fmt_layer);  // Filtered console output

    // Set as global default subscriber
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| SdkError::Other(anyhow::anyhow!("Failed to set tracing subscriber: {}", e)))?;

    // Initialize LogTracer to bridge log crate to tracing
    // This allows log::info!() calls to be forwarded to tracing (and thus OpenTelemetry)
    let _ = tracing_log::LogTracer::init();
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

    // Add tenant_id and deployment_id from global config
    if let Some(tid) = get_tenant_id() {
        attributes.push(KeyValue::new("tenant.id", tid.to_string()));
    }
    if let Some(did) = get_deployment_id() {
        attributes.push(KeyValue::new("deployment.id", did.to_string()));
    }

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
        // Pass through all metadata as span attributes
        // This allows Python code to set custom attributes like input.data, output.data, etc.
        for (key, value) in meta.iter() {
            // Map known keys to their canonical names
            let attr_key = match key.as_str() {
                "tenant_id" => "tenant.id".to_string(),
                "run_id" => "run.id".to_string(),
                "step_name" => "function.step_name".to_string(),
                "attempt" | "attempt_number" | "step_attempt" => {
                    // Try to parse as integer for step_attempt
                    if let Ok(parsed) = value.parse::<i64>() {
                        attributes.push(KeyValue::new("function.step_attempt", parsed));
                        continue;
                    }
                    "function.step_attempt".to_string()
                }
                "user_id" => "user.id".to_string(),
                "request_id" => "request.id".to_string(),
                // Pass through all other keys as-is (e.g., input.data, output.data, agent.name, tool.name)
                other => other.to_string(),
            };
            attributes.push(KeyValue::new(attr_key, value.clone()));
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

    // Add tenant_id and deployment_id from global config
    if let Some(tid) = get_tenant_id() {
        attributes.push(KeyValue::new("tenant.id", tid.to_string()));
    }
    if let Some(did) = get_deployment_id() {
        attributes.push(KeyValue::new("deployment.id", did.to_string()));
    }

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

/// Shutdown telemetry gracefully with timeout protection
///
/// This function shuts down telemetry and uses a 5-second timeout to prevent hanging forever.
/// Note: In OpenTelemetry 0.30+, global::shutdown_tracer_provider() was removed,
/// so this primarily serves as a clean shutdown point.
pub fn shutdown_telemetry() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    tracing::info!("Shutting down telemetry");

    // Create a channel for timeout handling
    let (tx, rx) = mpsc::channel();

    // Spawn shutdown in a separate thread to enforce timeout
    thread::spawn(move || {
        // Note: In OpenTelemetry 0.30+, global::shutdown_tracer_provider() was removed.
        // The batch span processor handles flushing automatically.

        // Signal completion
        let _ = tx.send(());
    });

    // Wait for shutdown with timeout
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(_) => {
            tracing::info!("Telemetry shutdown completed successfully");
        }
        Err(_) => {
            eprintln!("Warning: Telemetry shutdown timed out after 5 seconds");
            eprintln!("         Some telemetry data may not have been exported");
        }
    }
}

// =============================================================================
// Metrics
// =============================================================================

/// Static counter for execution requests received by the worker
static EXECUTION_REQUESTS_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();

/// Initialize the execution requests counter
/// This should be called after init_telemetry()
fn get_execution_requests_counter() -> &'static Counter<u64> {
    EXECUTION_REQUESTS_COUNTER.get_or_init(|| {
        let meter = global::meter("agnt5-sdk-core");
        meter
            .u64_counter("agnt5.worker.execution_requests")
            .with_description("Number of execution requests received by the worker")
            .with_unit("requests")
            .build()
    })
}

/// Record an execution request received by the worker
///
/// This increments the execution_requests counter with the given attributes.
/// Call this when a worker receives a new execution request (function, workflow, agent, etc.)
pub fn record_execution_request(component_name: &str, component_type: &str) {
    let counter = get_execution_requests_counter();
    let mut attrs = vec![
        KeyValue::new("component.name", component_name.to_string()),
        KeyValue::new("component.type", component_type.to_string()),
    ];

    // Add tenant_id and deployment_id from global config
    if let Some(tid) = get_tenant_id() {
        attrs.push(KeyValue::new("tenant.id", tid.to_string()));
    }
    if let Some(did) = get_deployment_id() {
        attrs.push(KeyValue::new("deployment.id", did.to_string()));
    }

    counter.add(1, &attrs);
}

/// Record an execution request with additional attributes
pub fn record_execution_request_with_attrs(
    component_name: &str,
    component_type: &str,
    additional_attrs: &[KeyValue],
) {
    let counter = get_execution_requests_counter();
    let mut attrs = vec![
        KeyValue::new("component.name", component_name.to_string()),
        KeyValue::new("component.type", component_type.to_string()),
    ];

    // Add tenant_id and deployment_id from global config
    if let Some(tid) = get_tenant_id() {
        attrs.push(KeyValue::new("tenant.id", tid.to_string()));
    }
    if let Some(did) = get_deployment_id() {
        attrs.push(KeyValue::new("deployment.id", did.to_string()));
    }

    attrs.extend_from_slice(additional_attrs);
    counter.add(1, &attrs);
}

// =============================================================================
// Reconnection Metrics
// =============================================================================

/// Histogram for time from disconnect to successful reconnect (client-perceived)
static RECONNECTION_DURATION_HISTOGRAM: OnceLock<Histogram<f64>> = OnceLock::new();

/// Counter for total reconnection attempts (success + failure)
static RECONNECTION_ATTEMPTS_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();

/// Gauge for current connection state (0=disconnected, 1=connecting, 2=connected)
static CONNECTION_STATE_GAUGE: OnceLock<Gauge<i64>> = OnceLock::new();

fn get_reconnection_duration_histogram() -> &'static Histogram<f64> {
    RECONNECTION_DURATION_HISTOGRAM.get_or_init(|| {
        let meter = global::meter("agnt5-sdk-core");
        meter
            .f64_histogram("agnt5.worker.reconnection.duration.seconds")
            .with_description("Time from disconnect to successful reconnect")
            .with_unit("s")
            .with_boundaries(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0])
            .build()
    })
}

fn get_reconnection_attempts_counter() -> &'static Counter<u64> {
    RECONNECTION_ATTEMPTS_COUNTER.get_or_init(|| {
        let meter = global::meter("agnt5-sdk-core");
        meter
            .u64_counter("agnt5.worker.reconnection.attempts.total")
            .with_description("Total reconnection attempts")
            .with_unit("attempts")
            .build()
    })
}

fn get_connection_state_gauge() -> &'static Gauge<i64> {
    CONNECTION_STATE_GAUGE.get_or_init(|| {
        let meter = global::meter("agnt5-sdk-core");
        meter
            .i64_gauge("agnt5.worker.connection.state")
            .with_description("Current connection state (0=disconnected, 1=connecting, 2=connected)")
            .build()
    })
}

/// Build common attributes for reconnection metrics
fn reconnection_attrs() -> Vec<KeyValue> {
    let mut attrs = Vec::new();
    if let Some(tid) = get_tenant_id() {
        attrs.push(KeyValue::new("tenant.id", tid.to_string()));
    }
    if let Some(did) = get_deployment_id() {
        attrs.push(KeyValue::new("deployment.id", did.to_string()));
    }
    attrs
}

/// Record the duration of a successful reconnection
pub fn record_reconnection_duration(duration_secs: f64) {
    let histogram = get_reconnection_duration_histogram();
    histogram.record(duration_secs, &reconnection_attrs());
}

/// Record a reconnection attempt (success or failure)
pub fn record_reconnection_attempt(success: bool) {
    let counter = get_reconnection_attempts_counter();
    let mut attrs = reconnection_attrs();
    attrs.push(KeyValue::new("success", success));
    counter.add(1, &attrs);
}

/// Update the current connection state gauge
/// 0 = disconnected, 1 = connecting, 2 = connected
pub fn update_connection_state(state: i64) {
    let gauge = get_connection_state_gauge();
    gauge.record(state, &reconnection_attrs());
}

// =============================================================================
// Checkpoint Metrics
// =============================================================================

/// Histogram for checkpoint round-trip duration (worker → EE → DB → worker)
static CHECKPOINT_DURATION_HISTOGRAM: OnceLock<Histogram<f64>> = OnceLock::new();

/// Counter for checkpoint events emitted
static CHECKPOINT_TOTAL_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();

fn get_checkpoint_duration_histogram() -> &'static Histogram<f64> {
    CHECKPOINT_DURATION_HISTOGRAM.get_or_init(|| {
        let meter = global::meter("agnt5-sdk-core");
        meter
            .f64_histogram("agnt5.worker.checkpoint.duration.seconds")
            .with_description("Round-trip duration of checkpoint gRPC calls from worker to platform")
            .with_unit("s")
            .with_boundaries(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ])
            .build()
    })
}

fn get_checkpoint_total_counter() -> &'static Counter<u64> {
    CHECKPOINT_TOTAL_COUNTER.get_or_init(|| {
        let meter = global::meter("agnt5-sdk-core");
        meter
            .u64_counter("agnt5.worker.checkpoint.total")
            .with_description("Total checkpoint events emitted by the worker")
            .with_unit("events")
            .build()
    })
}

/// Record a checkpoint round-trip duration and increment counter
pub fn record_checkpoint(
    event_type: &str,
    duration_secs: f64,
    success: bool,
    experiment_id: Option<&str>,
) {
    let mut attrs = vec![
        KeyValue::new("checkpoint.type", event_type.to_string()),
        KeyValue::new("success", success),
    ];

    if let Some(tid) = get_tenant_id() {
        attrs.push(KeyValue::new("tenant.id", tid.to_string()));
    }
    if let Some(did) = get_deployment_id() {
        attrs.push(KeyValue::new("deployment.id", did.to_string()));
    }
    if let Some(eid) = experiment_id {
        attrs.push(KeyValue::new("experiment.id", eid.to_string()));
    }

    get_checkpoint_duration_histogram().record(duration_secs, &attrs);
    get_checkpoint_total_counter().add(1, &attrs);
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
