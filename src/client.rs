use crate::error::{Result, SdkError};
use crate::pb::{
    worker_coordinator_service_client::WorkerCoordinatorServiceClient,
    ServiceMessage, RuntimeMessage,
};
use tonic::transport::Channel;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, debug};

/// Simple client for communicating with the Worker Coordinator service
#[derive(Debug, Clone)]
pub struct WorkerCoordinatorClient {
    client: WorkerCoordinatorServiceClient<Channel>,
}

impl WorkerCoordinatorClient {
    /// Create a new client connected to the Worker Coordinator
    pub async fn connect(endpoint: String) -> Result<Self> {
        info!("Connecting to Worker Coordinator at {}", endpoint);
        
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| SdkError::Connection(format!("Invalid endpoint {}: {}", endpoint, e)))?
            .connect()
            .await?;

        let client = WorkerCoordinatorServiceClient::new(channel);
        
        info!("Successfully connected to Worker Coordinator");
        Ok(Self { client })
    }

    /// Create a worker stream for bidirectional communication with the coordinator
    pub async fn create_worker_stream(
        &mut self,
    ) -> Result<(
        mpsc::Sender<ServiceMessage>,
        tokio_stream::wrappers::UnboundedReceiverStream<std::result::Result<RuntimeMessage, tonic::Status>>,
    )> {
        debug!("Creating worker stream for bidirectional communication");
        
        // Create channels for bidirectional communication
        let (tx, rx) = mpsc::channel(32);
        let request_stream = ReceiverStream::new(rx);
        
        // Start the bidirectional stream
        let response_stream = self
            .client
            .worker_stream(request_stream)
            .await?
            .into_inner();
        
        // Convert to unbounded receiver stream for easier handling
        let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();
        
        // Spawn a task to forward stream messages
        tokio::spawn(async move {
            let mut stream = response_stream;
            while let Some(message) = tokio_stream::StreamExt::next(&mut stream).await {
                if response_tx.send(message).is_err() {
                    break;
                }
            }
        });
        
        let response_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(response_rx);
        
        Ok((tx, response_stream))
    }
}