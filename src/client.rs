use crate::error::{Result, SdkError};
use crate::pb::{
    worker_coordinator_service_client::WorkerCoordinatorServiceClient, RegisterService,
    RuntimeMessage, ServiceMessage,
};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{error, info};

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

        Ok(Self { client })
    }

    /// Create a worker stream with immediate registration (based on working pattern)
    pub async fn create_worker_stream_with_registration(
        &mut self,
        worker_id: String,
        registration: RegisterService,
    ) -> Result<(
        flume::Sender<ServiceMessage>,
        flume::Receiver<RuntimeMessage>,
    )> {
        // Create the registration message first
        let registration_message = ServiceMessage {
            worker_id: worker_id.clone(),
            message_type: Some(crate::pb::service_message::MessageType::RegisterService(
                registration,
            )),
        };

        // Create bounded channels for ongoing communication (reasonable default capacity)
        let (outgoing_tx, outgoing_rx) = flume::bounded::<ServiceMessage>(1000);
        let (runtime_msg_tx, runtime_msg_rx) = flume::bounded::<RuntimeMessage>(1000);

        // Create stream that yields registration immediately, then handles ongoing messages
        let outgoing_stream = async_stream::stream! {
            // First, yield the registration message immediately
            yield registration_message;

            // Then, handle ongoing messages from the channel
            loop {
                match outgoing_rx.recv_async().await {
                    Ok(msg) => {
                        yield msg;
                    },
                    Err(_) => {
                        break;
                    }
                }
            }
        };

        // Establish the gRPC stream
        let mut response_stream = self
            .client
            .worker_stream(outgoing_stream)
            .await
            .map_err(|e| {
                error!("Failed to create gRPC worker stream: {}", e);
                SdkError::Connection(format!("gRPC stream failed: {}", e))
            })?
            .into_inner();

        let registration_response =
            tokio::time::timeout(Duration::from_secs(10), response_stream.message())
                .await
                .map_err(|_| {
                    error!("Timeout waiting for registration response");
                    SdkError::Connection(
                        "Registration timeout - no response from runtime".to_string(),
                    )
                })?
                .map_err(|e| {
                    error!("Failed to receive registration response: {}", e);
                    SdkError::Connection(format!("Stream error: {}", e))
                })?;

        // Process registration response
        if let Some(runtime_message) = registration_response {
            match &runtime_message.message_data {
                Some(crate::pb::runtime_message::MessageData::RegisterServiceResponse(resp)) => {
                    if !resp.ack {
                        error!("Registration failed: {}", resp.error);
                        return Err(SdkError::Connection(format!(
                            "Registration failed: {}",
                            resp.error
                        )));
                    }
                }
                _ => {
                    error!("Unexpected response type to registration");
                    return Err(SdkError::Connection(
                        "Unexpected response to registration".to_string(),
                    ));
                }
            }
        } else {
            error!("No registration response received");
            return Err(SdkError::Connection(
                "No registration response received".to_string(),
            ));
        }

        // Spawn simple task to forward stream messages to runtime channel
        tokio::spawn(async move {
            while let Some(message_result) =
                tokio_stream::StreamExt::next(&mut response_stream).await
            {
                match message_result {
                    Ok(runtime_message) => {
                        if runtime_msg_tx.send_async(runtime_message).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Stream error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok((outgoing_tx, runtime_msg_rx))
    }
}
