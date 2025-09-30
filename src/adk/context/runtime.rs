//! Runtime control wiring for durable operations.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Result, SdkError};
use crate::pb::{
    runtime_service_request::Operation as RuntimeOperation,
    runtime_service_response::Result as RuntimeResult, RunCheckpointRequest, RunFailRequest,
    RuntimeServiceRequest,
};

use super::utils::block_on_runtime;
use super::ContextRuntimeState;

#[derive(Debug, Clone, Default)]
pub struct RuntimeControls {
    state: Option<Arc<ContextRuntimeState>>,
}

impl RuntimeControls {
    pub(crate) fn with_state(state: Option<Arc<ContextRuntimeState>>) -> Self {
        Self { state }
    }

    fn state(&self) -> Result<Arc<ContextRuntimeState>> {
        self.state.clone().ok_or_else(|| {
            SdkError::Unavailable("durable runtime controls unavailable in this context".into())
        })
    }

    /// Persist a checkpoint for the current run.
    pub fn checkpoint(&self, note: Option<&str>) -> Result<()> {
        let state = self.state()?;
        let mut metadata: HashMap<String, String> = HashMap::new();
        if let Some(invocation) = state.invocation_id.as_ref() {
            metadata.insert("invocation_id".to_string(), invocation.clone());
        }

        let note_value = note.unwrap_or("").to_string();
        if !note_value.is_empty() {
            metadata.insert("note".to_string(), note_value.clone());
        }

        let client = Arc::clone(&state.client);
        let tenant_id = state.tenant_id.clone();
        let session_id = state.session_id.clone();
        let run_id = state.run_id.clone();
        let step_id = state.step_id.clone();
        let attempt = state.attempt;

        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id,
            session_id,
            operation: Some(RuntimeOperation::RunCheckpoint(RunCheckpointRequest {
                run_id,
                step_id: step_id.clone(),
                checkpoint_name: step_id.clone(),
                checkpoint_key: step_id,
                status: "SUCCEEDED".to_string(),
                attempt,
                result: Vec::new(),
                note: note_value,
                metadata,
            })),
        };

        block_on_runtime(async move {
            let response = client.request(request).await?;
            match response.result {
                Some(RuntimeResult::RunCheckpoint(_)) | None => Ok(()),
                _ => Err(SdkError::Internal(
                    "unexpected runtime response for checkpoint".into(),
                )),
            }
        })
    }

    /// Explicitly fail the current run with a reason.
    pub fn fail(&self, reason: &str) -> Result<()> {
        let state = self.state()?;
        let mut metadata: HashMap<String, String> = HashMap::new();
        if let Some(invocation) = state.invocation_id.as_ref() {
            metadata.insert("invocation_id".to_string(), invocation.clone());
        }

        let client = Arc::clone(&state.client);
        let tenant_id = state.tenant_id.clone();
        let session_id = state.session_id.clone();
        let run_id = state.run_id.clone();
        let step_id = state.step_id.clone();

        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id,
            session_id,
            operation: Some(RuntimeOperation::RunFail(RunFailRequest {
                run_id,
                step_id,
                reason: reason.to_string(),
                error_type: String::new(),
                metadata,
            })),
        };

        block_on_runtime(async move {
            let response = client.request(request).await?;
            match response.result {
                Some(RuntimeResult::RunFail(_)) | None => Ok(()),
                _ => Err(SdkError::Internal(
                    "unexpected runtime response for fail".into(),
                )),
            }
        })
    }
}
