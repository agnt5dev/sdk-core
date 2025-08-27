use crate::client::WorkerCoordinatorClient;
use crate::error::Result;
use crate::pb::{ServiceMessage, RuntimeMessage, RegisterService, ComponentInfo, HealthCheck, UnregisterService, WorkerHealthStatus};
use tracing::{info, debug, error, warn};
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Connection states for tracking worker status
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Error(String),
}

/// RAII guard to ensure heartbeat task is always cleaned up
struct HeartbeatGuard {
    task_handle: tokio::task::JoinHandle<()>,
}

impl HeartbeatGuard {
    fn new(task_handle: tokio::task::JoinHandle<()>) -> Self {
        Self { task_handle }
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        debug!("Cleaning up heartbeat task");
        self.task_handle.abort();
    }
}

/// Simple worker that connects to the Worker Coordinator
#[derive(Debug, Clone)]
pub struct Worker {
    worker_id: String,
    coordinator_endpoint: String,
    service_name: String,
    service_version: String,
    service_type: String,
    tenant_id: String,
    deployment_id: String,
    components: Vec<ComponentInfo>,
    connection_state: Arc<std::sync::Mutex<ConnectionState>>,
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
        
        // Get tenant and deployment from environment variables, default to "default"
        let tenant_id = std::env::var("AGNT5_TENANT_ID").unwrap_or_else(|_| "default".to_string());
        let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_else(|_| "default".to_string());
        
        info!("Creating worker {} for service {} (tenant: {}, deployment: {}) connecting to {}", 
              worker_id, service_name, tenant_id, deployment_id, coordinator_endpoint);

        Self {
            worker_id,
            coordinator_endpoint,
            service_name,
            service_version,
            service_type,
            tenant_id,
            deployment_id,
            components: vec![],
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Disconnected)),
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
        
        // Get tenant and deployment from environment variables, default to "default"
        let tenant_id = std::env::var("AGNT5_TENANT_ID").unwrap_or_else(|_| "default".to_string());
        let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_else(|_| "default".to_string());
        
        info!("Creating worker {} for service {} (tenant: {}, deployment: {}) with {} components", 
              worker_id, service_name, tenant_id, deployment_id, components.len());

        Self {
            worker_id,
            coordinator_endpoint,
            service_name,
            service_version,
            service_type,
            tenant_id,
            deployment_id,
            components,
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Disconnected)),
        }
    }

    /// Create a new worker with explicit tenant and deployment
    pub fn new_with_tenant(
        coordinator_endpoint: String,
        service_name: String,
        service_version: String,
        service_type: String,
        tenant_id: String,
        deployment_id: String,
    ) -> Self {
        let worker_id = Uuid::new_v4().to_string();
        info!("Creating worker {} for service {} (tenant: {}, deployment: {}) connecting to {}", 
              worker_id, service_name, tenant_id, deployment_id, coordinator_endpoint);

        Self {
            worker_id,
            coordinator_endpoint,
            service_name,
            service_version,
            service_type,
            tenant_id,
            deployment_id,
            components: vec![],
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Disconnected)),
        }
    }

    /// Get the worker ID
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Set components for the worker
    pub fn set_components(&mut self, components: Vec<ComponentInfo>) {
        self.components = components;
    }

    /// Get current connection state
    pub fn connection_state(&self) -> ConnectionState {
        self.connection_state
            .lock()
            .unwrap_or_else(|poisoned| {
                warn!("Connection state mutex poisoned, recovering");
                poisoned.into_inner()
            })
            .clone()
    }

    /// Set connection state
    fn set_connection_state(&self, state: ConnectionState) {
        let mut guard = self.connection_state
            .lock()
            .unwrap_or_else(|poisoned| {
                warn!("Connection state mutex poisoned during set, recovering");
                poisoned.into_inner()
            });
        *guard = state;
    }



    /// Run the worker with a message handler
    pub async fn run<F, Fut>(&self, mut message_handler: F) -> Result<()>
    where
        F: FnMut(RuntimeMessage) -> Fut + Send,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send,
    {
        info!("Starting worker {} with auto-reconnect", self.worker_id);
        
        // Setup shutdown signal handling
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();
        
        // Spawn signal handler
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("Received shutdown signal (Ctrl+C)");
            shutdown_flag_clone.store(true, Ordering::Relaxed);
        });
        
        // Retry configuration with jitter
        let max_retries = 5;
        let base_delay = std::time::Duration::from_secs(1);
        
        loop {
            // Check for shutdown signal
            if shutdown_flag.load(Ordering::Relaxed) {
                info!("Worker {} shutting down due to signal", self.worker_id);
                return Ok(());
            }
            
            for retry_count in 0..max_retries {
                // Check for shutdown signal
                if shutdown_flag.load(Ordering::Relaxed) {
                    info!("Worker {} shutting down due to signal", self.worker_id);
                    return Ok(());
                }
                
                // Exponential backoff with jitter
                let mut delay = base_delay * 2_u32.pow(retry_count);
                if retry_count > 0 {
                    // Add jitter (±25% of delay)
                    let jitter = rand::random::<f64>() * 0.5 - 0.25;
                    let jitter_ms = (delay.as_millis() as f64 * jitter) as u64;
                    delay = delay + std::time::Duration::from_millis(jitter_ms);
                    
                    info!("Worker {} reconnect attempt {} of {} (waiting {:?})", 
                          self.worker_id, retry_count + 1, max_retries, delay);
                    
                    // Simple interruptible sleep
                    tokio::time::sleep(delay).await;
                }
                
                // Try to connect and register
                self.set_connection_state(ConnectionState::Connecting);
                let shutdown_for_connect = shutdown_flag.clone();
                match self.connect_and_run(&mut message_handler, shutdown_for_connect).await {
                    Ok(()) => {
                        info!("Worker {} completed successfully", self.worker_id);
                        self.set_connection_state(ConnectionState::Disconnected);
                        return Ok(());
                    }
                    Err(e) => {
                        let error_msg = format!("Worker {} connection failed (attempt {}): {}", 
                                               self.worker_id, retry_count + 1, e);
                        error!("{}", error_msg);
                        self.set_connection_state(ConnectionState::Error(error_msg.clone()));
                        
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
            
            // Simple long sleep
            tokio::time::sleep(long_delay).await;
        }
    }

    /// Attempt to connect once and return immediately after registration
    pub async fn connect_and_run_once(&self) -> Result<()> {
        debug!("🔄 Testing connection to coordinator...");
        let mut client = WorkerCoordinatorClient::connect(self.coordinator_endpoint.clone()).await?;
        
        // Create registration message with components
        let registration = RegisterService {
            service_name: self.service_name.clone(),
            service_version: self.service_version.clone(),
            service_type: self.service_type.clone(),
            components: self.components.clone(),
            tenant_id: self.tenant_id.clone(),
            deployment_id: self.deployment_id.clone(),
            metadata: std::collections::HashMap::new(),
        };
        
        info!("Testing registration for worker {} with service {}", self.worker_id, self.service_name);
        
        // Test connection with immediate registration - we don't need the channels for this test
        let (_tx, _rx) = client.create_worker_stream_with_registration(self.worker_id.clone(), registration).await?;
        
        info!("✅ Connection test successful for worker {}", self.worker_id);
        Ok(())
    }

    /// Internal method to connect and run until disconnection
    async fn connect_and_run<F, Fut>(&self, message_handler: &mut F, shutdown_flag: Arc<AtomicBool>) -> Result<()>
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
            tenant_id: self.tenant_id.clone(),
            deployment_id: self.deployment_id.clone(),
            metadata: std::collections::HashMap::new(),
        };
        
        debug!("📝 Registration details: service_name={}, service_type={}, service_version={}, tenant_id={}, deployment_id={}", 
               self.service_name, self.service_type, self.service_version, self.tenant_id, self.deployment_id);
        
        info!("Registering worker {} with service {}", self.worker_id, self.service_name);
        
        // Use the working pattern - create stream with immediate registration
        let (tx, rx) = client.create_worker_stream_with_registration(self.worker_id.clone(), registration).await?;
        
        info!("✅ Worker {} registered successfully and connected", self.worker_id);
        self.set_connection_state(ConnectionState::Connected);
        
        // Start heartbeat task with RAII guard for cleanup
        let heartbeat_task = self.spawn_heartbeat_task(tx.clone());
        let _heartbeat_guard = HeartbeatGuard::new(heartbeat_task);
        
        info!("Worker {} is running and waiting for messages", self.worker_id);
        
        // Use reasonable timeout for message processing (30 seconds)
        let message_timeout = std::time::Duration::from_secs(30);
        
        // Continue with normal message handling using flume receiver
        loop {
            // Check for shutdown signal
            if shutdown_flag.load(Ordering::Relaxed) {
                info!("Worker {} received shutdown signal, stopping gracefully", self.worker_id);
                let _ = self.send_shutdown_message(&tx).await;
                return Ok(()); // HeartbeatGuard will clean up automatically
            }
            
            // Use configurable timeout for message processing
            match tokio::time::timeout(message_timeout, rx.recv_async()).await {
                Ok(Ok(runtime_message)) => {
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
                Ok(Err(e)) => {
                    error!("Channel error for worker {}, will reconnect: {}", self.worker_id, e);
                    
                    // Try to send graceful shutdown message
                    let _ = self.send_shutdown_message(&tx).await;
                    
                    // Return error to trigger reconnection (HeartbeatGuard will clean up)
                    return Err(crate::error::SdkError::Connection(format!("Receive failed: {}", e)));
                }
                Err(_) => {
                    // Timeout - check for shutdown and continue
                    if shutdown_flag.load(Ordering::Relaxed) {
                        info!("Worker {} shutting down after timeout check", self.worker_id);
                        let _ = self.send_shutdown_message(&tx).await;
                        return Ok(());
                    }
                    continue;
                }
            }
        }
    }

    /// Spawn a simple heartbeat task that sends periodic health checks
    fn spawn_heartbeat_task(&self, tx: flume::Sender<ServiceMessage>) -> tokio::task::JoinHandle<()> {
        let worker_id = self.worker_id.clone();
        
        info!("Starting heartbeat task for worker {} (interval: 30s)", worker_id);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                
                let health_check = HealthCheck {
                    timestamp,
                    status: WorkerHealthStatus::WorkerHealthHealthy.into(),
                    metrics: std::collections::HashMap::new(),
                    message: "Worker healthy".to_string(),
                };
                
                let service_message = ServiceMessage {
                    worker_id: worker_id.clone(),
                    message_type: Some(crate::pb::service_message::MessageType::HealthCheck(health_check)),
                };
                
                // Send heartbeat - if it fails, the channel is closed so we exit
                if tx.send_async(service_message).await.is_err() {
                    debug!("Heartbeat channel closed for worker {}", worker_id);
                    break;
                }
                
                debug!("Sent heartbeat for worker {}", worker_id);
            }
            
            info!("Heartbeat task ended for worker {}", worker_id);
        })
    }

    /// Send graceful shutdown message
    async fn send_shutdown_message(&self, tx: &flume::Sender<ServiceMessage>) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
            
        let unregister = UnregisterService {
            reason: "Worker shutdown".to_string(),
            timestamp,
        };
        
        let service_message = ServiceMessage {
            worker_id: self.worker_id.clone(),
            message_type: Some(crate::pb::service_message::MessageType::UnregisterService(unregister)),
        };
        
        match tx.send_async(service_message).await {
            Ok(_) => {
                info!("Sent graceful shutdown message for worker {}", self.worker_id);
                // Give a moment for the message to be processed
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                Ok(())
            }
            Err(e) => {
                warn!("Failed to send shutdown message for worker {}: {}", self.worker_id, e);
                Err(crate::error::SdkError::Connection(format!("Shutdown message failed: {}", e)))
            }
        }
    }
}