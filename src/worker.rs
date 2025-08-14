use crate::client::WorkerCoordinatorClient;
use crate::error::Result;
use crate::pb::{ServiceMessage, RuntimeMessage, RegisterService};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
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
        }
    }

    /// Get the worker ID
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }


    /// Create a stream connection to the coordinator
    pub async fn create_stream(&self) -> Result<(
        mpsc::Sender<ServiceMessage>,
        tokio_stream::wrappers::UnboundedReceiverStream<std::result::Result<RuntimeMessage, tonic::Status>>,
    )> {
        info!("Creating stream connection for worker {}", self.worker_id);
        
        let mut client = WorkerCoordinatorClient::connect(self.coordinator_endpoint.clone()).await?;
        let (tx, rx) = client.create_worker_stream().await?;
        
        info!("Stream connection established for worker {}", self.worker_id);
        Ok((tx, rx))
    }

    /// Run the worker with a message handler
    pub async fn run<F, Fut>(&self, mut message_handler: F) -> Result<()>
    where
        F: FnMut(RuntimeMessage) -> Fut + Send,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send,
    {
        info!("Starting worker {}", self.worker_id);
        
        let (tx, mut rx) = self.create_stream().await?;
        
        // Send registration message immediately
        let register_message = ServiceMessage {
            worker_id: self.worker_id.clone(),
            message_type: Some(crate::pb::service_message::MessageType::RegisterService(
                RegisterService {
                    service_name: self.service_name.clone(),
                    service_version: self.service_version.clone(),
                    service_type: self.service_type.clone(),
                    components: vec![], // Empty for now
                }
            )),
        };
        
        info!("Registering worker {} with service {}", self.worker_id, self.service_name);
        tx.send(register_message).await
            .map_err(|e| crate::error::SdkError::Worker(format!("Failed to send registration: {}", e)))?;
        
        // Wait for registration response
        if let Some(message_result) = rx.next().await {
            match message_result {
                Ok(runtime_message) => {
                    match runtime_message.message_data {
                        Some(crate::pb::runtime_message::MessageData::RegisterServiceResponse(response)) => {
                            if !response.ack {
                                return Err(crate::error::SdkError::Worker(
                                    format!("Registration failed: {}", response.error)
                                ));
                            }
                            info!("Worker {} registered successfully", self.worker_id);
                        }
                        _ => {
                            return Err(crate::error::SdkError::Worker(
                                "Expected registration response, got different message".to_string()
                            ));
                        }
                    }
                }
                Err(e) => {
                    return Err(crate::error::SdkError::Worker(
                        format!("Stream error during registration: {}", e)
                    ));
                }
            }
        } else {
            return Err(crate::error::SdkError::Worker(
                "Stream closed before registration response".to_string()
            ));
        }
        
        info!("Worker {} is running and waiting for messages", self.worker_id);
        
        // Continue with normal message handling
        while let Some(message_result) = rx.next().await {
            match message_result {
                Ok(runtime_message) => {
                    debug!("Received message for worker {}: {:?}", self.worker_id, runtime_message);
                    
                    // Call the user-provided message handler
                    match message_handler(runtime_message).await {
                        Ok(Some(response)) => {
                            // Send response if handler provided one
                            if let Err(e) = tx.send(response).await {
                                error!("Failed to send response from worker {}: {}", self.worker_id, e);
                            }
                        }
                        Ok(None) => {
                            // No response needed
                        }
                        Err(e) => {
                            error!("Message handler error in worker {}: {}", self.worker_id, e);
                        }
                    }
                }
                Err(e) => {
                    error!("Stream error for worker {}: {}", self.worker_id, e);
                    break;
                }
            }
        }
        
        info!("Worker {} stopped", self.worker_id);
        Ok(())
    }
}