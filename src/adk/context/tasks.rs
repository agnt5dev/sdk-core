//! Task control wiring for durable spawns.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Result, SdkError};
use crate::pb::{
    runtime_service_request::Operation as RuntimeOperation,
    runtime_service_response::Result as RuntimeResult, RuntimeServiceRequest, TaskSpawnRequest,
};

use super::utils::block_on_runtime;
use super::ContextRuntimeState;

#[derive(Debug, Clone, Default)]
pub struct TaskControls {
    state: Option<Arc<ContextRuntimeState>>,
}

impl TaskControls {
    pub(crate) fn with_state(state: Option<Arc<ContextRuntimeState>>) -> Self {
        Self { state }
    }

    fn state(&self) -> Result<Arc<ContextRuntimeState>> {
        self.state.clone().ok_or_else(|| {
            SdkError::Unavailable {
                message: "durable task controls unavailable in this context".into(),
                service: None,
            }
        })
    }

    /// Spawn a durable task/tool invocation.
    pub fn spawn(&self, target: &str) -> Result<()> {
        if target.is_empty() {
            return Err(SdkError::InvalidArgument {
                message: "spawn requires a non-empty task target".into(),
                argument: Some("target".to_string()),
            });
        }

        let state = self.state()?;

        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("task_target".to_string(), target.to_string());
        if let Some(invocation) = state.invocation_id.as_ref() {
            metadata.insert("parent_invocation_id".to_string(), invocation.clone());
        }

        let invocation_id = uuid::Uuid::new_v4().to_string();
        let dedupe_id = format!("{}:{}:{}", state.run_id, state.step_id, target);

        let client = Arc::clone(&state.client);
        let session_id = state.session_id.clone();
        let run_id = state.run_id.clone();
        let step_id = state.step_id.clone();

        let request = RuntimeServiceRequest {
            request_id: String::new(),
            session_id,
            operation: Some(RuntimeOperation::TaskSpawn(TaskSpawnRequest {
                run_id,
                step_id,
                task_target: target.to_string(),
                invocation_id: invocation_id.clone(),
                payload: Vec::new(),
                dedupe_id,
                metadata,
            })),
        };

        block_on_runtime(async move {
            let response = client.request(request).await?;
            match response.result {
                Some(RuntimeResult::TaskSpawn(result)) => {
                    if result.status.is_empty() || result.status == "ENQUEUED" {
                        Ok(())
                    } else {
                        Err(SdkError::State {
                            message: format!(
                                "task spawn returned unexpected status '{}': invocation {}",
                                result.status, result.invocation_id
                            ),
                            code: crate::error::ErrorCode::InvalidState,
                        })
                    }
                }
                _ => Err(SdkError::Internal(
                    "unexpected runtime response for task spawn".into(),
                )),
            }
        })
    }
}
