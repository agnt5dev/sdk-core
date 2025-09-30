use crate::client::WorkerCoordinatorClient;
use crate::error::{Result, SdkError};
use crate::pb::{
    runtime_message, service_message, RuntimeMessage, RuntimeServiceRequest,
    RuntimeServiceResponse, ServiceMessage,
};
use crate::worker::WorkerConfig;
use flume::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

/// Client that uses the WorkerStream to perform RuntimeService RPCs.
#[derive(Debug)]
pub struct RuntimeServiceClient {
    worker_id: String,
    sender: Sender<ServiceMessage>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<RuntimeServiceResponse>>>>,
    _listener: JoinHandle<()>,
}

impl RuntimeServiceClient {
    /// Connect to the WorkerCoordinator using environment-driven configuration.
    pub async fn connect_from_env() -> Result<Self> {
        let config = WorkerConfig::new(
            std::env::var("AGNT5_SERVICE_NAME")
                .unwrap_or_else(|_| "adk-runtime-client".to_string()),
            std::env::var("AGNT5_SERVICE_VERSION").unwrap_or_else(|_| "1.0.0".to_string()),
            "runtime-client".to_string(),
        );

        Self::connect(config).await
    }

    async fn connect(config: WorkerConfig) -> Result<Self> {
        let mut client =
            WorkerCoordinatorClient::connect(config.coordinator_endpoint.clone()).await?;

        let register = crate::pb::RegisterService {
            service_name: config.service_name.clone(),
            service_version: config.service_version.clone(),
            service_type: config.service_type.clone(),
            tenant_id: config.tenant_id.clone(),
            deployment_id: config.deployment_id.clone(),
            components: vec![],
            metadata: HashMap::new(),
        };

        let (sender, receiver) = client
            .create_worker_stream_with_registration(config.worker_id.clone(), register)
            .await?;

        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<RuntimeServiceResponse>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = Arc::clone(&pending);
        let listener = Self::spawn_listener(receiver, pending_clone);

        Ok(Self {
            worker_id: config.worker_id,
            sender,
            pending,
            _listener: listener,
        })
    }

    fn spawn_listener(
        receiver: Receiver<RuntimeMessage>,
        pending: Arc<Mutex<HashMap<String, oneshot::Sender<RuntimeServiceResponse>>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Ok(message) = receiver.recv_async().await {
                if let Some(runtime_service) = message.message_data.as_ref().and_then(|data| {
                    if let runtime_message::MessageData::RuntimeServiceResponse(resp) = data {
                        Some(resp.clone())
                    } else {
                        None
                    }
                }) {
                    let request_id = runtime_service.request_id.clone();
                    if let Some(sender) = pending
                        .lock()
                        .ok()
                        .and_then(|mut map| map.remove(&request_id))
                    {
                        let _ = sender.send(runtime_service);
                    } else {
                        warn!("No pending runtime service request for id {}", request_id);
                    }
                }
            }
        })
    }

    /// Perform a runtime service request and await the response.
    pub async fn request(
        &self,
        mut request: RuntimeServiceRequest,
    ) -> Result<RuntimeServiceResponse> {
        let request_id = if request.request_id.is_empty() {
            let id = Uuid::new_v4().to_string();
            request.request_id = id.clone();
            id
        } else {
            request.request_id.clone()
        };

        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| SdkError::Internal("runtime pending map poisoned".into()))?
            .insert(request_id.clone(), tx);

        self.sender
            .send_async(ServiceMessage {
                worker_id: self.worker_id.clone(),
                message_type: Some(service_message::MessageType::RuntimeService(request)),
            })
            .await
            .map_err(|err| SdkError::Connection(format!("send runtime service: {}", err)))?;

        let response = rx.await.map_err(|_| {
            SdkError::Internal("runtime service response channel closed unexpectedly".into())
        })?;

        if response.success {
            Ok(response)
        } else {
            let message = if response.error_message.is_empty() {
                "runtime service error".to_string()
            } else {
                response.error_message.clone()
            };
            Err(SdkError::State(message))
        }
    }
}

impl Drop for RuntimeServiceClient {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            for (_, sender) in pending.drain() {
                let _ = sender.send(RuntimeServiceResponse {
                    request_id: String::new(),
                    success: false,
                    error_message: "runtime client dropped".into(),
                    result: None,
                });
            }
        }
    }
}
