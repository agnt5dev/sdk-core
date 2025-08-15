use crate::error::{Result, SdkError};
use crate::pb::{
    worker_coordinator_service_client::WorkerCoordinatorServiceClient,
    ServiceMessage, RuntimeMessage, RegisterService,
};
use tonic::transport::Channel;
use tracing::{info, debug, error};
use std::time::Duration;

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

    /// Create a worker stream with immediate registration (based on working pattern)
    pub async fn create_worker_stream_with_registration(
        &mut self,
        worker_id: String,
        registration: RegisterService,
    ) -> Result<(flume::Sender<ServiceMessage>, flume::Receiver<RuntimeMessage>)> {
        debug!("Creating worker stream with immediate registration");
        
        // Create the registration message first
        let registration_message = ServiceMessage {
            worker_id: worker_id.clone(),
            message_type: Some(crate::pb::service_message::MessageType::RegisterService(registration)),
        };
        debug!("📝 Registration message created");
        
        // Create channels for ongoing communication
        let (outgoing_tx, outgoing_rx) = flume::unbounded::<ServiceMessage>();
        let (runtime_msg_tx, runtime_msg_rx) = flume::unbounded::<RuntimeMessage>();
        
        // Create stream that yields registration immediately, then handles ongoing messages
        let outgoing_stream = async_stream::stream! {
            // First, yield the registration message immediately
            debug!("📤 Yielding registration message to stream");
            yield registration_message;
            
            // Then, handle ongoing messages from the channel
            loop {
                match outgoing_rx.recv_async().await {
                    Ok(msg) => {
                        debug!("📤 Yielding ongoing message to stream");
                        yield msg;
                    },
                    Err(_) => {
                        debug!("📪 Outgoing channel closed, ending stream");
                        break;
                    }
                }
            }
        };
        
        debug!("🔄 Initiating gRPC bidirectional stream with registration...");
        
        // Establish the gRPC stream
        let mut response_stream = self
            .client
            .worker_stream(outgoing_stream)
            .await
            .map_err(|e| {
                error!("❌ Failed to create gRPC worker stream: {}", e);
                SdkError::Connection(format!("gRPC stream failed: {}", e))
            })?
            .into_inner();
        
        debug!("✅ gRPC bidirectional stream established successfully");
        
        // Wait for registration response with timeout
        debug!("⏳ Waiting for registration acknowledgment...");
        let registration_response = tokio::time::timeout(
            Duration::from_secs(10),
            response_stream.message()
        )
        .await
        .map_err(|_| {
            error!("❌ Timeout waiting for registration response");
            SdkError::Connection("Registration timeout - no response from runtime".to_string())
        })?
        .map_err(|e| {
            error!("❌ Failed to receive registration response: {}", e);
            SdkError::Connection(format!("Stream error: {}", e))
        })?;
        
        // Process registration response
        if let Some(runtime_message) = registration_response {
            debug!("📥 Received registration response");
            match &runtime_message.message_data {
                Some(crate::pb::runtime_message::MessageData::RegisterServiceResponse(resp)) => {
                    if resp.ack {
                        info!("✅ Registration successful!");
                    } else {
                        error!("❌ Registration failed: {}", resp.error);
                        return Err(SdkError::Connection(format!("Registration failed: {}", resp.error)));
                    }
                }
                _ => {
                    error!("❌ Unexpected response type to registration");
                    return Err(SdkError::Connection("Unexpected response to registration".to_string()));
                }
            }
        } else {
            error!("❌ No registration response received");
            return Err(SdkError::Connection("No registration response received".to_string()));
        }
        
        // Spawn task to forward remaining stream messages to runtime channel
        tokio::spawn(async move {
            while let Some(message_result) = tokio_stream::StreamExt::next(&mut response_stream).await {
                match message_result {
                    Ok(runtime_message) => {
                        debug!("📨 Forwarding runtime message");
                        if runtime_msg_tx.send_async(runtime_message).await.is_err() {
                            debug!("📪 Runtime message channel closed, stopping forwarder");
                            break;
                        }
                    }
                    Err(e) => {
                        error!("❌ Stream error: {}", e);
                        break;
                    }
                }
            }
            debug!("🔚 Message forwarder completed");
        });
        
        debug!("✅ Worker stream with registration completed successfully");
        Ok((outgoing_tx, runtime_msg_rx))
    }
}