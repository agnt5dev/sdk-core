use agnt5_sdk_core::{Worker, Result, pb::ComponentInfo};
use tracing::{info, error, Level};
use tracing_subscriber::{self, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize clean logging - filter out noisy dependencies
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("agnt5_sdk_core=info".parse().unwrap())
                .add_directive("debug_worker=info".parse().unwrap())
                .add_directive("h2=warn".parse().unwrap())
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("tonic=warn".parse().unwrap())
                .add_directive("tower=warn".parse().unwrap())
        )
        .init();

    info!("Starting debug worker");

    // Create a test component
    let test_component = ComponentInfo {
        name: "test_handler".to_string(),
        component_type: 1, // COMPONENT_TYPE_FUNCTION
        input_schema: None, // For simplicity, no schema validation in debug
        output_schema: None,
        config: std::collections::HashMap::new(),
        metadata: std::collections::HashMap::new(),
    };

    let worker = Worker::new(
        "debug-test-service".to_string(),
        "1.0.0".to_string(),
        "debug".to_string(),
        vec![test_component],
    );


    // Create a simple handler that echoes back the message
    let handler = |message| {
        async move {
            info!("Handler received message: {:?}", message);
            
            // For testing purposes, we can echo back a simple response
            // In a real application, you would process the message and return appropriate response
            Ok(None) // No response needed for this debug test
        }
    };

    // Run the worker indefinitely - it will handle reconnections automatically
    match worker.run(handler).await {
        Ok(()) => {
            info!("Worker completed successfully");
        }
        Err(e) => {
            error!("Worker failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}