//! Signal control wiring for durable waits.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Result, SdkError};
use crate::pb::{
    runtime_service_request::Operation as RuntimeOperation,
    runtime_service_response::Result as RuntimeResult, RuntimeServiceRequest, SignalWaitRequest,
};

use super::utils::block_on_runtime;
use super::ContextRuntimeState;

#[derive(Debug, Clone, Default)]
pub struct SignalControls {
    state: Option<Arc<ContextRuntimeState>>,
}

impl SignalControls {
    pub(crate) fn with_state(state: Option<Arc<ContextRuntimeState>>) -> Self {
        Self { state }
    }

    fn state(&self) -> Result<Arc<ContextRuntimeState>> {
        self.state.clone().ok_or_else(|| SdkError::Unavailable {
            message: "durable signal controls unavailable in this context".into(),
            service: None,
        })
    }

    /// Register a wait for a named signal.
    pub fn wait(&self, name: &str, correlation: Option<&str>) -> Result<()> {
        if name.is_empty() {
            return Err(SdkError::InvalidArgument {
                message: "signal wait requires a non-empty signal name".into(),
                argument: Some("name".to_string()),
            });
        }

        let state = self.state()?;
        let wait_id = format!("{}:{name}", state.step_id);
        let dedupe_id = format!("{}:{}:{name}", state.run_id, state.step_id);

        let mut metadata: HashMap<String, String> = HashMap::new();
        if let Some(correlation) = correlation {
            metadata.insert("correlation_id".to_string(), correlation.to_string());
        }
        if let Some(invocation) = state.invocation_id.as_ref() {
            metadata.insert("invocation_id".to_string(), invocation.clone());
        }

        let client = Arc::clone(&state.client);
        let session_id = state.session_id.clone();
        let run_id = state.run_id.clone();
        let step_id = state.step_id.clone();

        let request = RuntimeServiceRequest {
            request_id: String::new(),
            session_id,
            operation: Some(RuntimeOperation::SignalWait(SignalWaitRequest {
                run_id,
                step_id,
                signal_name: name.to_string(),
                wait_id,
                correlation_id: correlation.unwrap_or_default().to_string(),
                dedupe_id,
                timeout_ms: 0,
                auto_ack: true,
                metadata,
            })),
        };

        block_on_runtime(async move {
            let response = client.request(request).await?;
            match response.result {
                Some(RuntimeResult::SignalWait(result)) => {
                    if result.delivered {
                        Ok(())
                    } else {
                        Err(SdkError::State {
                            message: "signal wait completed without delivery".into(),
                            code: crate::error::ErrorCode::InvalidState,
                        })
                    }
                }
                _ => Err(SdkError::Internal(
                    "unexpected runtime response for signal wait".into(),
                )),
            }
        })
    }
}
