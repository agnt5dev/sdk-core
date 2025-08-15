use agnt5_sdk_core::{Worker, Result};
use tracing::{info, debug, error, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize detailed logging
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    info!("🚀 Starting Rust SDK debug test");

    let worker = Worker::new(
        "http://[::1]:9091".to_string(), // WorkerCoordinatorService runs on port 9091
        "debug-test-service".to_string(),
        "1.0.0".to_string(),
        "debug".to_string(),
    );

    info!("🔧 Worker created: ID={}", worker.worker_id());

    // Create a simple handler that just logs received messages
    let handler = |message| {
        async move {
            info!("📥 Handler received message: {:?}", message);
            Ok(None) // No response needed for this test
        }
    };

    info!("🏃 Starting worker run loop...");
    
    // Run the worker indefinitely - it will wait for messages from the server
    match worker.run(handler).await {
        Ok(()) => {
            info!("✅ Worker completed successfully");
        }
        Err(e) => {
            error!("❌ Worker failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}