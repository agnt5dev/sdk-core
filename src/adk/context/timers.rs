//! Timer control wiring for durable sleeps.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{Result, SdkError};
use crate::pb::{
    runtime_service_request::Operation as RuntimeOperation,
    runtime_service_response::Result as RuntimeResult, RuntimeServiceRequest, TimerSleepRequest,
};

use super::utils::block_on_runtime;
use super::ContextRuntimeState;

#[derive(Debug, Clone, Default)]
pub struct TimerControls {
    state: Option<Arc<ContextRuntimeState>>,
}

impl TimerControls {
    pub(crate) fn with_state(state: Option<Arc<ContextRuntimeState>>) -> Self {
        Self { state }
    }

    fn state(&self) -> Result<Arc<ContextRuntimeState>> {
        self.state.clone().ok_or_else(|| {
            SdkError::Unavailable {
                message: "durable timer controls unavailable in this context".into(),
                service: None,
            }
        })
    }

    /// Schedule a sleep for the given duration.
    pub fn sleep(&self, duration: Duration) -> Result<()> {
        if duration.is_zero() {
            return Ok(());
        }

        let state = self.state()?;
        let delay_ms = i64::try_from(duration.as_millis()).map_err(|_| {
            SdkError::InvalidArgument {
                message: "timer duration exceeds maximum supported range".into(),
                argument: Some("duration".to_string()),
            }
        })?;

        let timer_key = format!("{}:sleep:{}", state.step_id, delay_ms);
        let dedupe_id = format!("{}:{}:{}", state.run_id, state.step_id, timer_key);

        let client = Arc::clone(&state.client);
        let tenant_id = state.tenant_id.clone();
        let session_id = state.session_id.clone();
        let run_id = state.run_id.clone();
        let step_id = state.step_id.clone();

        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id,
            session_id,
            operation: Some(RuntimeOperation::TimerSleep(TimerSleepRequest {
                run_id,
                step_id,
                timer_key,
                delay_ms,
                fire_at: None,
                dedupe_id,
                metadata: Default::default(),
            })),
        };

        block_on_runtime(async move {
            let response = client.request(request).await?;
            match response.result {
                Some(RuntimeResult::TimerSleep(_)) | None => Ok(()),
                _ => Err(SdkError::Internal(
                    "unexpected runtime response for timer sleep".into(),
                )),
            }
        })
    }
}
