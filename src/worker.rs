use crate::client::WorkerCoordinatorClient;
use crate::error::Result;
use crate::pb::{ServiceMessage, RuntimeMessage, RegisterService, ComponentInfo};
use tracing::{info, debug, error};
use uuid::Uuid;

/// Simple worker that connects to the Worker Coordinator
#[derive(Debug)]
pub struct Worker {
    worker_id: String,
    coordinator_endpoint: String,
    service_name: String,
    service_version: String,
    service_type: String,
    components: Vec<ComponentInfo>,
}

impl Worker {
    /// Create a new worker
    pub fn new(
        coordinator_endpoint: String,
        service_name: String,
        service_version: String,
        service_type: String,
    ) -> Self {
        let worker_id = Uuid::new_v4().to_string();
        info!("Creating worker {} for service {} connecting to {}", worker_id, service_name, coordinator_endpoint);

        Self {
            worker_id,
            coordinator_endpoint,
            service_name,
            service_version,
            service_type,
            components: vec![],
        }
    }

    /// Create a new worker with components
    pub fn new_with_components(
        coordinator_endpoint: String,
        service_name: String,
        service_version: String,
        service_type: String,
        components: Vec<ComponentInfo>,
    ) -> Self {
        let worker_id = Uuid::new_v4().to_string();
        info!("Creating worker {} for service {} with {} components", worker_id, service_name, components.len());

        Self {
            worker_id,
            coordinator_endpoint,
            service_name,
            service_version,
            service_type,
            components,
        }
    }

    /// Get the worker ID
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }



    /// Run the worker with a message handler
    pub async fn run<F, Fut>(&self, mut message_handler: F) -> Result<()>
    where
        F: FnMut(RuntimeMessage) -> Fut + Send,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send,
    {
        info!("Starting worker {} with auto-reconnect", self.worker_id);
        
        // Retry configuration
        let max_retries = 5;
        let base_delay = std::time::Duration::from_secs(1);
        
        loop {
            for retry_count in 0..max_retries {
                let delay = base_delay * 2_u32.pow(retry_count); // Exponential backoff
                
                if retry_count > 0 {
                    info!("Worker {} reconnect attempt {} of {} (waiting {:?})", 
                          self.worker_id, retry_count + 1, max_retries, delay);
                    tokio::time::sleep(delay).await;
                }
                
                // Try to connect and register
                match self.connect_and_run(&mut message_handler).await {
                    Ok(()) => {
                        info!("Worker {} completed successfully", self.worker_id);
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Worker {} connection failed (attempt {}): {}", 
                               self.worker_id, retry_count + 1, e);
                        
                        if retry_count == max_retries - 1 {
                            error!("Worker {} max retries exceeded, backing off", self.worker_id);
                            break;
                        }
                    }
                }
            }
            
            // After max retries, wait longer before trying again
            let long_delay = std::time::Duration::from_secs(30);
            info!("Worker {} waiting {:?} before retrying connection", self.worker_id, long_delay);
            tokio::time::sleep(long_delay).await;
        }
    }

    /// Internal method to connect and run until disconnection
    async fn connect_and_run<F, Fut>(&self, message_handler: &mut F) -> Result<()>
    where
        F: FnMut(RuntimeMessage) -> Fut + Send,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send,
    {
        debug!("🔄 Creating stream connection with immediate registration...");
        let mut client = WorkerCoordinatorClient::connect(self.coordinator_endpoint.clone()).await?;
        
        // Create registration message with components
        let registration = RegisterService {
            service_name: self.service_name.clone(),
            service_version: self.service_version.clone(),
            service_type: self.service_type.clone(),
            components: self.components.clone(),
        };
        
        debug!("📝 Registration details: service_name={}, service_type={}, service_version={}", 
               self.service_name, self.service_type, self.service_version);
        
        info!("Registering worker {} with service {}", self.worker_id, self.service_name);
        
        // Use the working pattern - create stream with immediate registration
        let (tx, rx) = client.create_worker_stream_with_registration(self.worker_id.clone(), registration).await?;
        
        info!("✅ Worker {} registered successfully and connected", self.worker_id);
        
        info!("Worker {} is running and waiting for messages", self.worker_id);
        
        // Continue with normal message handling using flume receiver
        loop {
            match rx.recv_async().await {
                Ok(runtime_message) => {
                    debug!("Received message for worker {}: {:?}", self.worker_id, runtime_message);
                    
                    // Call the user-provided message handler
                    match message_handler(runtime_message).await {
                        Ok(Some(response)) => {
                            // Send response back to coordinator
                            debug!("Sending response back to coordinator");
                            if let Err(e) = tx.send_async(response).await {
                                error!("Failed to send response for worker {}: {}", self.worker_id, e);
                                // Connection lost, return error to trigger reconnection
                                return Err(crate::error::SdkError::Connection(format!("Send failed: {}", e)));
                            }
                        }
                        Ok(None) => {
                            // No response needed
                            debug!("Handler completed without response");
                        }
                        Err(e) => {
                            error!("Message handler error in worker {}: {}", self.worker_id, e);
                        }
                    }
                }
                Err(e) => {
                    error!("Channel error for worker {}, will reconnect: {}", self.worker_id, e);
                    // Return error to trigger reconnection
                    return Err(crate::error::SdkError::Connection(format!("Receive failed: {}", e)));
                }
            }
        }
    }
}