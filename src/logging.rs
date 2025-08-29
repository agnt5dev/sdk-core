use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use tracing_subscriber::{EnvFilter, fmt};
use tracing_subscriber::prelude::*;
use tracing_log::LogTracer;

// Global error buffer for capturing errors to send to Python
lazy_static::lazy_static! {
    static ref ERROR_BUFFER: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::with_capacity(100)));
}

/// Initialize logging with a custom error capturing layer
pub fn init_logging() -> Result<(), Box<dyn std::error::Error>> {
    // Check if already initialized
    static INIT: std::sync::Once = std::sync::Once::new();
    let mut result = Ok(());
    
    INIT.call_once(|| {
        // Initialize log -> tracing compatibility
        if let Err(e) = LogTracer::init() {
            eprintln!("Warning: Failed to initialize LogTracer: {}", e);
        }
        
        // Create env filter from RUST_LOG or default to info
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));
        
        // Create a layer that captures errors
        let error_capture_layer = ErrorCaptureLayer;
        
        // Build the subscriber with log compatibility
        let subscriber = tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_target(false))
            .with(error_capture_layer);
        
        // Set as global default
        if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
            result = Err(Box::new(e) as Box<dyn std::error::Error>);
        }
    });
    
    result
}

/// Get recent errors from the buffer
pub fn get_error_buffer() -> Vec<String> {
    ERROR_BUFFER.lock().unwrap().iter().cloned().collect()
}

/// Clear the error buffer
pub fn clear_error_buffer() {
    ERROR_BUFFER.lock().unwrap().clear();
}

/// Custom layer that captures error-level events
struct ErrorCaptureLayer;

impl<S> tracing_subscriber::Layer<S> for ErrorCaptureLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        // Only capture ERROR level events
        if event.metadata().level() == &tracing::Level::ERROR {
            // Format the error message
            let mut visitor = ErrorVisitor::default();
            event.record(&mut visitor);
            
            if let Some(message) = visitor.message {
                // Add to buffer
                let mut buffer = ERROR_BUFFER.lock().unwrap();
                
                // Keep buffer size limited
                if buffer.len() >= 100 {
                    buffer.pop_front();
                }
                
                // Add timestamp and message
                let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
                buffer.push_back(format!("[{}] {}", timestamp, message));
            }
        }
    }
}

/// Visitor to extract message from events
#[derive(Default)]
struct ErrorVisitor {
    message: Option<String>,
}

impl tracing::field::Visit for ErrorVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }
    
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        }
    }
}