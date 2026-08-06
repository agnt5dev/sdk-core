use crate::{
    error::Result,
    pb::BeginActivationRequest,
    runtime_adapter::{ActivationAdapter, ActivationDecision},
};

/// Minimal activation surface required by [`super::TimerNamespace`].
///
/// The trait keeps durable timer tests deterministic and lets embedders share
/// their existing Engine activation adapter without another transport.
#[async_trait::async_trait]
pub trait TimerActivationClient: Send + Sync + std::fmt::Debug {
    async fn begin_timer(&self, request: BeginActivationRequest) -> Result<ActivationDecision>;
}

#[async_trait::async_trait]
impl TimerActivationClient for tokio::sync::Mutex<ActivationAdapter> {
    async fn begin_timer(&self, request: BeginActivationRequest) -> Result<ActivationDecision> {
        self.lock().await.begin(request).await
    }
}
